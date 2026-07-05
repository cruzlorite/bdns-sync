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

"""Generic SCD2 table shape shared by every synced endpoint, plus control tables."""

from typing import Tuple

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
)


def build_sync_table(name: str, metadata: MetaData) -> Table:
    """Build the fixed generic SCD2 table for one synced endpoint. No per-field schema."""
    return Table(
        name,
        metadata,
        Column("_id", Integer, primary_key=True, autoincrement=True),
        Column("_natural_key", String, nullable=False, index=True),
        Column("_row_hash", String(64), nullable=False),
        Column("_valid_from", DateTime(timezone=True), nullable=False),
        Column("_valid_to", DateTime(timezone=True), nullable=True),
        Column("_is_current", Boolean, nullable=False),
        Column("_synced_at", DateTime(timezone=True), nullable=False),
        Column("payload", JSON, nullable=False),
        extend_existing=True,
    )


def build_staging_table(name: str, metadata: MetaData) -> Table:
    """Scratch table for one sync run's fetched batch -- cleared before use,
    diffed against the real table via bulk SQL instead of per-row loops.
    """
    return Table(
        f"_staging_{name}",
        metadata,
        Column("_natural_key", String, nullable=False, index=True),
        Column("_row_hash", String(64), nullable=False),
        Column("payload", JSON, nullable=False),
        extend_existing=True,
    )


def build_control_tables(metadata: MetaData) -> Tuple[Table, Table]:
    """`_sync_state` (per-table watermark) and `_sync_runs` (append-only run log)."""
    sync_state = Table(
        "_sync_state",
        metadata,
        Column("table_name", String, primary_key=True),
        Column("last_synced_at", DateTime(timezone=True), nullable=True),
        Column("last_run_id", Integer, nullable=True),
        extend_existing=True,
    )
    sync_runs = Table(
        "_sync_runs",
        metadata,
        Column("run_id", Integer, primary_key=True, autoincrement=True),
        Column("table_name", String, nullable=False, index=True),
        Column("run_type", String, nullable=False),
        Column("started_at", DateTime(timezone=True), nullable=False),
        Column("finished_at", DateTime(timezone=True), nullable=True),
        Column("rows_fetched", Integer, nullable=False, default=0),
        Column("rows_inserted", Integer, nullable=False, default=0),
        Column("rows_closed", Integer, nullable=False, default=0),
        Column("status", String, nullable=False),
        Column("error", String, nullable=True),
        extend_existing=True,
    )
    return sync_state, sync_runs
