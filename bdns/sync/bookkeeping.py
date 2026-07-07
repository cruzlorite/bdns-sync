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

import logging
import time
from datetime import datetime, timezone
from typing import Callable, Dict

from sqlalchemy import MetaData, insert, update
from sqlalchemy.engine import Engine

from bdns.sync.dialects import get_adapter
from bdns.sync.schema import build_control_tables, build_staging_table, build_sync_table

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def run_with_bookkeeping(
    engine: Engine, endpoint_name: str, run_type: str, apply_fn: Callable
) -> Dict[str, int]:
    """`run_type` is a classification label for `_sync_runs`: either "full"
    (full-catalog/swept/discover-then-detail syncs) or a reg-date window name
    (daily/weekly/monthly/annual, for the incremental syncs).
    """
    metadata = MetaData()
    table = build_sync_table(endpoint_name, metadata)
    staging = build_staging_table(endpoint_name, metadata)
    sync_state, sync_runs, sync_errors = build_control_tables(metadata)
    get_adapter(engine).prepare_metadata(metadata)
    metadata.create_all(engine, checkfirst=True)

    started_at = datetime.now(timezone.utc)
    # App-generated id (epoch microseconds) instead of DB autoincrement:
    # BigQuery has none, and this key is read back (it links `_sync_errors`
    # and the final status update). One id per run and runs are sequential
    # per orchestrator, so microseconds can't collide.
    run_id = time.time_ns() // 1_000
    logger.info("%s: run %s (%s) starting", endpoint_name, run_id, run_type)

    with engine.begin() as conn:
        conn.execute(
            insert(sync_runs).values(
                run_id=run_id,
                table_name=endpoint_name,
                run_type=run_type,
                started_at=started_at,
                status="running",
                rows_fetched=0,
                rows_inserted=0,
                rows_soft_deleted=0,
            )
        )

        try:
            stats = apply_fn(conn, table, staging)
        except Exception as exc:
            logger.error("%s: run %s failed after %.1fs: %s", endpoint_name, run_id,
                         (datetime.now(timezone.utc) - started_at).total_seconds(), exc)
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
        skip_details = stats.pop("_skip_details", [])
        conn.execute(
            update(sync_runs)
            .where(sync_runs.c.run_id == run_id)
            .values(
                finished_at=finished_at,
                status="success",
                rows_fetched=stats["fetched"],
                rows_inserted=stats["inserted"] + stats["updated"],
                rows_soft_deleted=stats.get("soft_deleted", 0),
                rows_skipped=stats.get("skipped", 0),
            )
        )
        if skip_details:
            conn.execute(
                insert(sync_errors),
                [
                    {
                        "error_id": run_id + i,
                        "run_id": run_id,
                        "table_name": endpoint_name,
                        "context": detail["context"],
                        "content": detail["content"],
                        "occurred_at": finished_at,
                    }
                    for i, detail in enumerate(skip_details)
                ],
            )
        _upsert_sync_state(conn, sync_state, endpoint_name, finished_at, run_id)

    logger.info(
        "%s: run %s done in %.1fs (fetched=%d inserted=%d updated=%d touched=%d soft_deleted=%d skipped=%d)",
        endpoint_name, run_id, (finished_at - started_at).total_seconds(),
        stats["fetched"], stats["inserted"], stats["updated"], stats["touched"],
        stats.get("soft_deleted", 0), stats.get("skipped", 0),
    )
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
