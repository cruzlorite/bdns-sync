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

"""Per-target adapters. The rest of the codebase writes plain, portable SQL
(see scd2.py) that runs unchanged on SQLite, Postgres, MySQL, and BigQuery.
This module is the one place that's allowed to know a specific engine's
name and quirks; nothing outside it should ever branch on `dialect.name`.

BigQuery is a first-class target, not an edge case: it's expected to run
every endpoint at real volume (concesiones_busqueda alone is 20M+ rows).
It's also, so far, the only target that needs an adapter at all - SQLite,
Postgres and MySQL are all covered by the DialectAdapter default.

To support a new quirk (for this or another engine): add a method to
DialectAdapter with a portable default (usually a no-op), override it in
the relevant adapter subclass, and call it from whichever module hits the
difference. `prepare_metadata` below is the existing example: BigQuery has
no secondary indexes (CREATE INDEX is rejected outright), so its adapter
strips them from the metadata before create_all; every other target keeps
them via the inherited no-op.

Scope note: this abstraction covers SQL engines only - anything that
speaks SQLAlchemy Engine (Redshift, Snowflake, DuckDB, ... would slot in
here as adapters). File-based targets (Parquet, Delta, ...) are a
different axis entirely: no connection, no UPDATE, no transaction, so the
staging+diff design in scd2.py doesn't apply to them. If one ever becomes
a real target, it needs its own Sink interface designed around that case,
not a DialectAdapter subclass.
"""

from typing import Any, Dict, Sequence, Type

from sqlalchemy import MetaData, insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql.schema import Table


class DialectAdapter:
    """Default adapter: assumes standard SQL support. Used for SQLite,
    Postgres, MySQL, and anything else without its own adapter below.
    """

    def prepare_metadata(self, metadata: MetaData) -> None:
        """Adjust a freshly-built MetaData for this target before create_all."""

    def insert_rows(self, conn: Connection, table: Table, rows: Sequence[Dict[str, Any]]) -> None:
        """Bulk-insert one batch of rows (scd2 staging load)."""
        conn.execute(insert(table), rows)


class BigQueryAdapter(DialectAdapter):
    def prepare_metadata(self, metadata: MetaData) -> None:
        for table in metadata.tables.values():
            table.indexes.clear()

    def insert_rows(self, conn: Connection, table: Table, rows: Sequence[Dict[str, Any]]) -> None:
        """Load job instead of DML INSERT. An earlier version batched DML
        INSERT statements (2,000 rows/statement) -- a real improvement over
        one-job-per-row, but still bottlenecked on DML job overhead:
        measured live, ~250-325 rows/s sustained. A load job writes the
        whole batch in one job with no per-statement cost, and -- unlike
        DML -- load jobs are free: they don't count against the query/DML
        byte quota or billing. Measured live: ~900 rows/s, ~3x the DML
        version.

        Blocks on `.result()` deliberately, even though it's tempting to
        submit jobs without waiting so the next batch's API fetch can run
        while this one is still loading server-side (BigQuery jobs are
        already async, so no thread/queue would even be needed for that).
        Tried it live: BigQuery caps table *update* operations (loads
        count) at a low rate regardless of whether earlier ones finished,
        and unblocked submission blew straight through it --
        `429 too many table update operations for this table`, a hard
        platform limit, not a quota this project can raise. The blocking
        `.result()` call is what was keeping submissions naturally paced
        under that limit; it stays.

        Bypasses SQLAlchemy's INSERT compilation and bind processors
        entirely, so payload serialization is done by hand here, reusing
        the staging table's own `payload` column type (`PortableJSON`) so
        the two paths can never drift out of sync with each other.
        """
        from google.cloud import bigquery

        client = conn.connection.driver_connection._client
        table_ref = bigquery.DatasetReference(client.project, conn.engine.url.database).table(table.name)
        payload_type = table.c.payload.type

        json_rows = []
        for row in rows:
            json_row = {
                "_natural_key": row["_natural_key"],
                "_row_hash": row["_row_hash"],
                "payload": payload_type.process_bind_param(row["payload"], None),
            }
            if "_reg_date" in row:
                json_row["_reg_date"] = row["_reg_date"].isoformat()
            json_rows.append(json_row)

        client.load_table_from_json(json_rows, table_ref).result()


_ADAPTERS: Dict[str, Type[DialectAdapter]] = {
    "bigquery": BigQueryAdapter,
}


def get_adapter(engine: Engine) -> DialectAdapter:
    return _ADAPTERS.get(engine.dialect.name, DialectAdapter)()
