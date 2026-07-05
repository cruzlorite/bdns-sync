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
target table with a fixed number of bulk SQL statements -- never a per-row
UPDATE/INSERT loop.

That distinction is not just an optimization. On Postgres/SQLite a loop of
per-row UPDATEs works fine; on BigQuery (a real target for this project)
every DML statement has real per-statement latency/cost regardless of how
many rows it touches, so a loop of thousands of individual UPDATEs per sync
run is a bad fit at the volumes involved (concesiones_busqueda alone is
20M+ rows). Staging + a handful of bulk statements costs the same number of
statements whether the batch is 20 rows or 2 million.

Only portable SQL is used -- correlated EXISTS/NOT EXISTS subqueries, no
vendor-specific UPDATE...FROM or MERGE syntax -- so the same code path runs
unchanged on SQLite, Postgres, MySQL, and BigQuery.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Sequence

from sqlalchemy import delete, exists, func, insert, literal, or_, select, update
from sqlalchemy.engine import Connection
from sqlalchemy.sql.schema import Table

from bdns.sync.hashing import natural_key, row_hash


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

    The last case is what detects deletions (grants withdrawn, retired codes) --
    incremental passes alone can't see removals, only a full-set diff can.
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
) -> Dict[str, int]:
    """
    Apply a partial/windowed batch of fetched rows (one reg-date window pass).

    Unlike `apply_full_reconciliation`, this never closes out keys absent from
    `rows` -- a reg-date window is a subset of the table, not the full current
    state, so absence here says nothing about deletion. Deletion detection is
    the job of a separate full-reconciliation pass.
    """
    return _apply(
        conn, table, staging, rows, key_fields, exclude_hash_fields, chunk_size,
        detect_deletions=False,
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
) -> Dict[str, int]:
    now = datetime.now(timezone.utc)

    conn.execute(delete(staging))
    fetched = _load_staging(conn, staging, rows, key_fields, exclude_hash_fields, chunk_size)
    stats = _diff_stats(conn, table, staging, detect_deletions)
    stats["fetched"] = fetched

    _touch_unchanged(conn, table, staging, now)
    _close_stale(conn, table, staging, now, detect_deletions)
    _insert_new_versions(conn, table, staging, now)

    conn.execute(delete(staging))
    return stats


def _load_staging(
    conn: Connection,
    staging: Table,
    rows: Iterable[Dict[str, Any]],
    key_fields: Sequence[str],
    exclude_hash_fields: Optional[Iterable[str]],
    chunk_size: int,
) -> int:
    fetched = 0
    chunk = []
    for payload in rows:
        chunk.append(
            {
                "_natural_key": natural_key(payload, key_fields),
                "_row_hash": row_hash(payload, exclude_hash_fields),
                "payload": payload,
            }
        )
        if len(chunk) >= chunk_size:
            conn.execute(insert(staging), chunk)
            fetched += len(chunk)
            chunk = []
    if chunk:
        conn.execute(insert(staging), chunk)
        fetched += len(chunk)
    return fetched


def _matches(table: Table, staging: Table):
    return staging.c._natural_key == table.c._natural_key


def _diff_stats(conn: Connection, table: Table, staging: Table, detect_deletions: bool) -> Dict[str, int]:
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
        stats["closed"] = closed

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
    conn: Connection, table: Table, staging: Table, now: datetime, detect_deletions: bool
) -> None:
    changed = exists(
        select(1).where(_matches(table, staging), staging.c._row_hash != table.c._row_hash)
    )
    condition = changed
    if detect_deletions:
        missing = ~exists(select(1).where(_matches(table, staging)))
        condition = or_(changed, missing)

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
        literal(None),
        literal(True),
        literal(now),
        staging.c.payload,
    ).where(no_current_match)

    conn.execute(
        insert(table).from_select(
            ["_natural_key", "_row_hash", "_valid_from", "_valid_to", "_is_current", "_synced_at", "payload"],
            select_new_versions,
        )
    )
