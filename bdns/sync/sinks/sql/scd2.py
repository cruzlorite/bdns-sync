# -*- coding: utf-8 -*-
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
# You should have received a copy of the GNU General Public License along
# with this program. If not, see <https://www.gnu.org/licenses/>.

"""Core SCD2 apply logic: stage the fetched batch, then diff it against the
target table with a fixed number of bulk SQL statements, never a per-row
UPDATE/INSERT loop.

That distinction is not just an optimization. On Postgres and SQLite a loop
of per-row UPDATEs works fine, but on BigQuery (a real target for this
project) every DML statement has real per-statement latency and cost
regardless of how many rows it touches. A loop of thousands of individual
UPDATEs per sync run is a bad fit at the volumes involved (concesiones_busqueda
alone is 20M+ rows). Staging plus a handful of bulk statements costs the
same number of statements whether the batch is 20 rows or 2 million.

Only portable SQL is used: correlated EXISTS/NOT EXISTS subqueries, no
vendor-specific UPDATE...FROM or MERGE syntax. That's what lets the same
code path run unchanged on SQLite, Postgres, MySQL, and BigQuery.
"""

import logging
import queue
import threading
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, Optional, Sequence

from sqlalchemy import and_, delete, exists, func, insert, literal, or_, select, true, update
from sqlalchemy.engine import Connection
from sqlalchemy.sql.schema import Table

from bdns.sync.sinks.sql.dialects import get_adapter
from bdns.sync.hashing import natural_key, row_hash

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def apply_full_reconciliation(
    conn: Connection,
    table: Table,
    staging: Table,
    rows: Iterable[Dict[str, Any]],
    key_fields: Sequence[str],
    exclude_hash_fields: Optional[Iterable[str]] = None,
    chunk_size: int = 5000,
) -> Dict[str, int]:
    """
    Diff a full batch of currently-fetched rows against the table's current rows.

    - Natural key not seen before -> insert new current row.
    - Natural key seen, hash changed -> close out old version, insert new one.
    - Natural key seen, hash unchanged -> just touch `_synced_at`.
    - Natural key was current but absent from `rows` -> close it out.

    The last case is what detects deletions (grants withdrawn, retired
    codes). Incremental passes alone can't see removals; only a full-set
    diff can.
    """
    return _apply(
        conn, table, staging, rows, key_fields, exclude_hash_fields, chunk_size,
        detect_deletions=True,
    )


def apply_incremental(
    conn: Connection,
    table: Table,
    staging: Table,
    rows: Iterable[Dict[str, Any]],
    key_fields: Sequence[str],
    exclude_hash_fields: Optional[Iterable[str]] = None,
    chunk_size: int = 5000,
    reg_date_field: Optional[str] = None,
    window_start: Optional[date] = None,
    window_end: Optional[date] = None,
) -> Dict[str, int]:
    """
    Apply a partial/windowed batch of fetched rows (one reg-date window pass).

    By default this never closes out keys absent from `rows`. A reg-date
    window is a subset of the table, not the full current state, so absence
    here says nothing about deletion on its own.

    Pass `reg_date_field`, along with `window_start` and `window_end` (the
    same bounds used to fetch `rows`), to opt into window-scoped deletion
    detection: a current row is closed only if its own stored `_reg_date`
    falls inside `[window_start, window_end]` and it's missing from `rows`.
    Scoping the comparison to rows that themselves belong to this window,
    rather than rows that were simply in the previous run's fetch, avoids
    the false-positive trap of a plain window-vs-window diff. Every row
    ages out of a rolling window eventually regardless of deletion, so that
    kind of comparison can't tell the two apart. This one can, because both
    sides of the comparison use the same fixed date range.
    """
    window = (reg_date_field, window_start, window_end) if reg_date_field else None
    return _apply(
        conn, table, staging, rows, key_fields, exclude_hash_fields, chunk_size,
        detect_deletions=False, window=window,
    )


def _apply(
    conn: Connection,
    table: Table,
    staging: Table,
    rows: Iterable[Dict[str, Any]],
    key_fields: Sequence[str],
    exclude_hash_fields: Optional[Iterable[str]],
    chunk_size: int,
    detect_deletions: bool,
    window: Optional[tuple] = None,
) -> Dict[str, int]:
    now = datetime.now(timezone.utc)
    reg_date_field = window[0] if window else None

    conn.execute(delete(staging).where(true()))
    fetched = _load_staging(conn, staging, rows, key_fields, exclude_hash_fields, chunk_size, reg_date_field)
    logger.info("%s: fetch done, %d rows staged, applying diff", table.name, fetched)
    stats = _diff_stats(conn, table, staging, detect_deletions, window)
    stats["fetched"] = fetched

    _touch_unchanged(conn, table, staging, now)
    _close_stale(conn, table, staging, now, detect_deletions, window)
    _insert_new_versions(conn, table, staging, now)

    conn.execute(delete(staging).where(true()))
    return stats


def _load_staging(
    conn: Connection,
    staging: Table,
    rows: Iterable[Dict[str, Any]],
    key_fields: Sequence[str],
    exclude_hash_fields: Optional[Iterable[str]],
    chunk_size: int,
    reg_date_field: Optional[str] = None,
) -> int:
    """Producer/consumer split so the API fetch of the next chunk overlaps
    with the database write of the current one, instead of the fetch
    sitting idle for the duration of every write (which is seconds per
    batch on BigQuery).

    The PRODUCER thread consumes `rows` (the API fetch generator, which has
    no thread affinity) and turns each record into a staged dict; full
    chunks go on a small bounded queue. The CONSUMER is this function, on
    the caller's own thread: it drains the queue and writes each chunk via
    `adapter.insert_rows`. Threading it this way round -- fetch on the
    helper thread, writes on the caller's thread -- is what keeps `conn`
    on the single thread that created it, which SQLite hard-requires
    (its DBAPI objects are thread-affine: using one from another thread
    raises, confirmed live). The write-on-helper-thread arrangement would
    need a per-dialect capability flag just to exist; this one is safe for
    every dialect unconditionally.

    Writes stay strictly one-at-a-time (each `insert_rows` finishes before
    the next chunk is taken). That pace matters on BigQuery: it caps table
    update operations (load jobs included) at a low fixed rate regardless
    of whether earlier ones finished, so concurrent writes trip `429 too
    many table update operations for this table` -- a hard platform limit
    (measured live), not a raisable quota. Serial writes paced by each
    job's own duration stay comfortably under it.

    The bounded queue is the backpressure: if writes fall behind, the
    producer blocks on `put` instead of buffering the whole backfill in
    memory.
    """
    adapter = get_adapter(conn.engine)
    chunk_queue: "queue.Queue" = queue.Queue(maxsize=2)
    _DONE = object()
    consumer_failed = threading.Event()

    def produce():
        try:
            chunk = []
            for payload in rows:
                staged = {
                    "_natural_key": natural_key(payload, key_fields),
                    "_row_hash": row_hash(payload, exclude_hash_fields),
                    "payload": payload,
                }
                if reg_date_field:
                    staged["_reg_date"] = datetime.strptime(payload[reg_date_field], "%Y-%m-%d").date()
                chunk.append(staged)
                if len(chunk) >= chunk_size:
                    if not put(chunk):
                        return  # consumer died; its exception is the real error
                    chunk = []
            if chunk:
                put(chunk)
            put(_DONE)
        except Exception as exc:  # fetch failure: hand it to the consumer to re-raise
            put(exc)

    def put(item) -> bool:
        """Blocks until queued, re-checking every second whether the
        consumer died -- otherwise a full queue would hang the producer
        forever instead of letting the consumer's error surface.
        """
        while not consumer_failed.is_set():
            try:
                chunk_queue.put(item, timeout=1)
                return True
            except queue.Full:
                continue
        return False

    producer = threading.Thread(target=produce, daemon=True)
    producer.start()

    fetched = 0
    try:
        while True:
            item = chunk_queue.get()
            if item is _DONE:
                break
            if isinstance(item, Exception):
                raise item
            adapter.insert_rows(conn, staging, item)
            fetched += len(item)
            # every ~50k rows: staging a multi-million-row backfill takes
            # hours, so it can't be a silent gap between the per-chunk fetch
            # logs and the final "fetch done" line.
            if fetched % (chunk_size * 10) == 0:
                logger.info("%s: %d rows staged so far", staging.name, fetched)
    except BaseException:
        consumer_failed.set()  # unblock the producer so it can exit
        raise
    finally:
        producer.join()
    return fetched


def _matches(table: Table, staging: Table):
    return staging.c._natural_key == table.c._natural_key


def _missing_in_window(table: Table, staging: Table, window: tuple):
    """A current row is eligible to close under window-scoped deletion only
    if its own `_reg_date` says it belongs to this exact window, regardless
    of whether it was in a previous run's fetch. Both sides of the
    comparison use the same range, so aging out of a rolling window is
    never mistaken for deletion.
    """
    _, start, end = window
    return and_(
        table.c._reg_date.isnot(None),
        table.c._reg_date >= start,
        table.c._reg_date <= end,
        ~exists(select(1).where(_matches(table, staging))),
    )


def _diff_stats(
    conn: Connection, table: Table, staging: Table, detect_deletions: bool, window: Optional[tuple] = None
) -> Dict[str, int]:
    touched = conn.execute(
        select(func.count())
        .select_from(table)
        .where(
            table.c._is_current.is_(True),
            exists(select(1).where(_matches(table, staging), staging.c._row_hash == table.c._row_hash)),
        )
    ).scalar_one()

    updated = conn.execute(
        select(func.count())
        .select_from(table)
        .where(
            table.c._is_current.is_(True),
            exists(select(1).where(_matches(table, staging), staging.c._row_hash != table.c._row_hash)),
        )
    ).scalar_one()

    inserted = conn.execute(
        select(func.count())
        .select_from(staging)
        .where(
            ~exists(
                select(1).where(_matches(table, staging), table.c._is_current.is_(True))
            )
        )
    ).scalar_one()

    stats = {"inserted": inserted, "updated": updated, "touched": touched}

    if detect_deletions:
        closed = conn.execute(
            select(func.count())
            .select_from(table)
            .where(table.c._is_current.is_(True), ~exists(select(1).where(_matches(table, staging))))
        ).scalar_one()
        stats["soft_deleted"] = closed
    elif window:
        closed = conn.execute(
            select(func.count())
            .select_from(table)
            .where(table.c._is_current.is_(True), _missing_in_window(table, staging, window))
        ).scalar_one()
        stats["soft_deleted"] = closed

    return stats


def _touch_unchanged(conn: Connection, table: Table, staging: Table, now: datetime) -> None:
    conn.execute(
        update(table)
        .where(
            table.c._is_current.is_(True),
            exists(select(1).where(_matches(table, staging), staging.c._row_hash == table.c._row_hash)),
        )
        .values(_synced_at=now)
    )


def _close_stale(
    conn: Connection,
    table: Table,
    staging: Table,
    now: datetime,
    detect_deletions: bool,
    window: Optional[tuple] = None,
) -> None:
    changed = exists(
        select(1).where(_matches(table, staging), staging.c._row_hash != table.c._row_hash)
    )
    condition = changed
    if detect_deletions:
        missing = ~exists(select(1).where(_matches(table, staging)))
        condition = or_(changed, missing)
    elif window:
        condition = or_(changed, _missing_in_window(table, staging, window))

    conn.execute(
        update(table)
        .where(table.c._is_current.is_(True), condition)
        .values(_valid_to=now, _is_current=False)
    )


def _insert_new_versions(conn: Connection, table: Table, staging: Table, now: datetime) -> None:
    no_current_match = ~exists(
        select(1).where(_matches(table, staging), table.c._is_current.is_(True))
    )
    select_new_versions = select(
        staging.c._natural_key,
        staging.c._row_hash,
        literal(now),
        literal(None, type_=table.c._valid_to.type),
        literal(True),
        literal(now),
        staging.c._reg_date,
        staging.c.payload,
    ).where(no_current_match)

    conn.execute(
        insert(table).from_select(
            [
                "_natural_key",
                "_row_hash",
                "_valid_from",
                "_valid_to",
                "_is_current",
                "_synced_at",
                "_reg_date",
                "payload",
            ],
            select_new_versions,
        )
    )
