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

"""Shared plumbing every group's `sync_*` function runs through: create
tables, log the run in `_sync_runs`, apply the group's own fetch+SCD2 logic,
record the outcome, and bump the `_sync_state` watermark.
"""

from datetime import datetime, timezone
from typing import Callable, Dict

from sqlalchemy import MetaData, insert, update
from sqlalchemy.engine import Engine

from bdns.sync.schema import build_control_tables, build_staging_table, build_sync_table


def run_with_bookkeeping(
    engine: Engine, endpoint_name: str, run_type: str, apply_fn: Callable
) -> Dict[str, int]:
    """`run_type` is a classification label for `_sync_runs` -- either "full"
    (full-catalog/swept/discover-then-detail syncs) or a reg-date window name
    (daily/weekly/monthly/annual, for the incremental syncs).
    """
    metadata = MetaData()
    table = build_sync_table(endpoint_name, metadata)
    staging = build_staging_table(endpoint_name, metadata)
    sync_state, sync_runs = build_control_tables(metadata)
    metadata.create_all(engine, checkfirst=True)

    started_at = datetime.now(timezone.utc)

    with engine.begin() as conn:
        run_id = conn.execute(
            insert(sync_runs).values(
                table_name=endpoint_name,
                run_type=run_type,
                started_at=started_at,
                status="running",
                rows_fetched=0,
                rows_inserted=0,
                rows_closed=0,
            )
        ).inserted_primary_key[0]

        try:
            stats = apply_fn(conn, table, staging)
        except Exception as exc:
            conn.execute(
                update(sync_runs)
                .where(sync_runs.c.run_id == run_id)
                .values(
                    finished_at=datetime.now(timezone.utc),
                    status="failed",
                    error=str(exc),
                )
            )
            raise

        finished_at = datetime.now(timezone.utc)
        conn.execute(
            update(sync_runs)
            .where(sync_runs.c.run_id == run_id)
            .values(
                finished_at=finished_at,
                status="success",
                rows_fetched=stats["fetched"],
                rows_inserted=stats["inserted"] + stats["updated"],
                rows_closed=stats.get("closed", 0),
            )
        )
        _upsert_sync_state(conn, sync_state, endpoint_name, finished_at, run_id)

    return stats


def _upsert_sync_state(conn, sync_state, table_name: str, synced_at: datetime, run_id: int) -> None:
    """Dialect-agnostic upsert: update if the row exists, insert otherwise."""
    result = conn.execute(
        update(sync_state)
        .where(sync_state.c.table_name == table_name)
        .values(last_synced_at=synced_at, last_run_id=run_id)
    )
    if result.rowcount == 0:
        conn.execute(
            insert(sync_state).values(
                table_name=table_name, last_synced_at=synced_at, last_run_id=run_id
            )
        )
