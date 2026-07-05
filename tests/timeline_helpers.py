"""Shared plumbing for the multi-day scenario tests (test_timeline_*.py).

Every scenario test scripts a sequence of simulated sync runs ("days")
against a `FakeBDNSClient` seeded from real, anonymized BDNS payloads
(tests/fixtures/), mutating the client's in-memory data between runs the
same way the real upstream data changes between cron runs: a field edited,
a row withdrawn, a new row registered.
"""

from copy import deepcopy
from typing import Any, Dict, Sequence

from sqlalchemy import MetaData, select
from bdns.sync.schema import build_sync_table


def current_rows(engine, name):
    metadata = MetaData()
    table = build_sync_table(name, metadata)
    with engine.begin() as conn:
        return conn.execute(select(table).where(table.c._is_current.is_(True))).mappings().all()


def all_rows(engine, name):
    metadata = MetaData()
    table = build_sync_table(name, metadata)
    with engine.begin() as conn:
        return conn.execute(select(table)).mappings().all()


def fresh_copy_with_new_key(row: Dict[str, Any], key_fields: Sequence[str]) -> Dict[str, Any]:
    """Deep-copy `row` and perturb its key field(s) so it reads as a brand
    new natural key -- used to simulate a newly-registered record appearing.
    """
    new_row = deepcopy(row)
    for field in key_fields:
        value = new_row[field]
        new_row[field] = value + 999000 if isinstance(value, int) else f"{value}-NEW"
    return new_row
