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

"""Storage abstraction: everything above this package fetches rows; a Sink
persists them.

The boundary is deliberately batch-oriented, not row-oriented. A sink is
never asked to "insert this row" or "update that one" -- it is handed the
complete batch of rows one sync run fetched, plus enough context to
version them (key fields, and for windowed runs the date range), and it
owns everything storage-side from there: table/file creation, SCD2
versioning, deletion detection, run logging, error records. That contract
is the widest one every conceivable target can honor: an UPDATE-capable
SQL engine implements it with staging plus bulk diff statements, while an
append-only target (a future Parquet sink) would implement the same
contract with partition writes and compaction. Anything narrower (per-row
CRUD) would silently assume SQL and make non-SQL sinks impossible.

Currently implemented: `sql.SQLSink`, covering every target with a
SQLAlchemy dialect (SQLite, PostgreSQL, MySQL, BigQuery, ...). Per-engine
quirks live inside that package (see `sql.dialects`); they never leak
through this interface.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence


class Sink(ABC):
    """One sync target (a database, a file store, ...) in SCD2 form.

    Implementations must guarantee, for every synced endpoint:

    - **SCD2 semantics.** Each record is identified by its natural key
      (the JSON-serialized tuple of `key_fields` values). A new key adds a
      current version; a changed payload (detected by content hash) closes
      the old version and adds a new current one; an unchanged payload only
      refreshes its last-seen timestamp. Closed versions are never deleted:
      history is append-only.
    - **Deletion detection scoped to what the batch can prove.** A
      full-catalog batch (`sync_full`) is the complete current state, so a
      key missing from it is a real deletion and its version is closed. A
      windowed batch (`sync_window`) is only a slice, so absence proves
      nothing by default; if `reg_date_field` is given, a stored row is
      closed only when its own registration date falls inside the window
      and it is absent from the batch (see `sync_window`).
    - **Atomicity per run.** Either the whole batch is applied and the run
      is recorded as successful, or the target's synced data is left
      untouched and the run is recorded as failed. No partially-applied
      batch may ever be visible to readers.
    - **Run bookkeeping.** Every call is logged storage-side (identifier,
      endpoint, run type, timestamps, row counters, final status, error
      text on failure), along with a per-endpoint watermark of the last
      successful run, and one record per skipped malformed row. How that
      log is stored is the sink's business; callers never see it.

    Stats contract: both methods return a dict with at least the keys
    `fetched` (rows consumed from `rows`), `inserted` (new versions
    written, whether for new keys or changed payloads), `updated` (changed
    payloads, i.e. the subset of `inserted` that closed a previous
    version), `touched` (unchanged rows re-seen), and `soft_deleted`
    (versions closed due to detected deletion). `skipped` is present when
    a `skipped` list was passed.
    """

    @abstractmethod
    def sync_full(
        self,
        endpoint: str,
        rows: Iterable[Dict[str, Any]],
        key_fields: Sequence[str],
        *,
        skipped: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, int]:
        """Reconcile `endpoint` against `rows` as its COMPLETE current state.

        Args:
            endpoint: Logical entity name; also the storage-side name (table,
                file prefix, ...) the sink keeps this entity under.
            rows: The full set of currently-existing records, as returned by
                the API. May be a lazy generator; it is consumed exactly once.
                Records are stored whole -- the sink never interprets payload
                fields beyond extracting `key_fields`.
            key_fields: Payload field names whose values form the natural key.
                Order matters (it is part of the serialized key).
            skipped: Optional list of malformed-record descriptors (dicts with
                `context` and `content` keys). The caller may keep appending
                to it WHILE `rows` is being consumed (the typical pattern is a
                generator that skips bad records and logs them here); the sink
                reads it only after `rows` is exhausted, persists each entry
                to its error log linked to this run, and reports the count as
                `skipped` in the returned stats.

        Returns:
            Stats dict (see class docstring). Because `rows` is the complete
            state, keys absent from it are closed and counted in
            `soft_deleted`.
        """

    @abstractmethod
    def sync_window(
        self,
        endpoint: str,
        rows: Iterable[Dict[str, Any]],
        key_fields: Sequence[str],
        *,
        window_start: date,
        window_end: date,
        run_type: str,
        reg_date_field: Optional[str] = None,
        skipped: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, int]:
        """Apply `rows` as the slice of `endpoint` registered in a date range.

        Unlike `sync_full`, absence from `rows` proves nothing on its own:
        the batch is a window over the entity, not its full state, so by
        default no version is ever closed. Versioning of the keys that ARE
        present works exactly as in `sync_full`.

        Args:
            endpoint: Same as in `sync_full`.
            rows: Every record whose registration date falls in
                `[window_start, window_end]` (both inclusive), as fetched
                from the API. Lazy generators welcome, consumed once.
            key_fields: Same as in `sync_full`.
            window_start: First day of the fetched range, inclusive.
            window_end: Last day of the fetched range, inclusive. Both bounds
                are recorded so windowed deletion detection can compare like
                with like (see `reg_date_field`).
            run_type: Label for the run log distinguishing cadence runs
                ("daily", "weekly", "monthly", "annual") from historical
                loads ("backfill"). The sink stores it verbatim.
            reg_date_field: Opt-in for window-scoped deletion detection: the
                payload field (ISO date string) holding the record's OWN
                registration date. When given, a stored current version is
                closed if its registration date falls inside
                `[window_start, window_end]` and its key is absent from
                `rows` -- both sides of the comparison then cover the same
                fixed range, which is what makes absence meaningful. Entities
                whose payload exposes no such field must leave this None and
                get no deletion detection on windowed runs.
            skipped: Same contract as in `sync_full`.

        Returns:
            Stats dict (see class docstring). `soft_deleted` can only be
            non-zero when `reg_date_field` was given.
        """


def get_sink(url: str) -> Sink:
    """Build the sink for a target URL. The URL scheme picks the
    implementation; today every scheme is a SQLAlchemy dialect and maps to
    `sql.SQLSink` (e.g. `sqlite:///...`, `postgresql://...`,
    `bigquery://project/dataset`). A future file-based sink would claim its
    own scheme here (e.g. `parquet:///path`) -- this factory is the single
    place that decision lives.
    """
    from bdns.sync.sinks.sql import SQLSink

    return SQLSink.from_url(url)
