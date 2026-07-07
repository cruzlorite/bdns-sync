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

from typing import Dict, Type

from sqlalchemy import MetaData
from sqlalchemy.engine import Engine


class DialectAdapter:
    """Default adapter: assumes standard SQL support. Used for SQLite,
    Postgres, MySQL, and anything else without its own adapter below.
    """

    def prepare_metadata(self, metadata: MetaData) -> None:
        """Adjust a freshly-built MetaData for this target before create_all."""


class BigQueryAdapter(DialectAdapter):
    def prepare_metadata(self, metadata: MetaData) -> None:
        for table in metadata.tables.values():
            table.indexes.clear()


_ADAPTERS: Dict[str, Type[DialectAdapter]] = {
    "bigquery": BigQueryAdapter,
}


def get_adapter(engine: Engine) -> DialectAdapter:
    return _ADAPTERS.get(engine.dialect.name, DialectAdapter)()
