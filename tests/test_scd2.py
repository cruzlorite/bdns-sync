import pytest
from sqlalchemy import MetaData, create_engine, select

from bdns.sync.scd2 import apply_full_reconciliation, apply_incremental
from bdns.sync.schema import build_staging_table, build_sync_table


@pytest.fixture
def table():
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    tbl = build_sync_table("things", metadata)
    staging = build_staging_table("things", metadata)
    metadata.create_all(engine)
    return engine, tbl, staging


def current_rows(conn, table):
    return conn.execute(
        select(table).where(table.c._is_current.is_(True))
    ).mappings().all()


def test_first_pass_inserts_all_rows(table):
    engine, tbl, staging = table
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    with engine.begin() as conn:
        stats = apply_full_reconciliation(conn, tbl, staging, rows, ("id",))
        assert stats == {"fetched": 2, "inserted": 2, "updated": 0, "touched": 0, "closed": 0}
        assert len(current_rows(conn, tbl)) == 2


def test_second_pass_with_no_changes_only_touches(table):
    engine, tbl, staging = table
    rows = [{"id": 1, "name": "a"}]
    with engine.begin() as conn:
        apply_full_reconciliation(conn, tbl, staging, rows, ("id",))
    with engine.begin() as conn:
        stats = apply_full_reconciliation(conn, tbl, staging, rows, ("id",))
        assert stats == {"fetched": 1, "inserted": 0, "updated": 0, "touched": 1, "closed": 0}
        assert len(current_rows(conn, tbl)) == 1


def test_changed_row_closes_old_version_and_inserts_new(table):
    engine, tbl, staging = table
    with engine.begin() as conn:
        apply_full_reconciliation(conn, tbl, staging, [{"id": 1, "name": "a"}], ("id",))
    with engine.begin() as conn:
        stats = apply_full_reconciliation(conn, tbl, staging, [{"id": 1, "name": "b"}], ("id",))
        assert stats == {"fetched": 1, "inserted": 0, "updated": 1, "touched": 0, "closed": 0}

    with engine.begin() as conn:
        all_rows = conn.execute(select(tbl)).mappings().all()
        assert len(all_rows) == 2  # old closed version + new current version
        current = [r for r in all_rows if r["_is_current"]]
        closed = [r for r in all_rows if not r["_is_current"]]
        assert len(current) == 1
        assert current[0]["payload"]["name"] == "b"
        assert len(closed) == 1
        assert closed[0]["_valid_to"] is not None


def test_missing_row_is_closed_out_as_deletion(table):
    engine, tbl, staging = table
    with engine.begin() as conn:
        apply_full_reconciliation(conn, tbl, staging, [{"id": 1}, {"id": 2}], ("id",))
    with engine.begin() as conn:
        stats = apply_full_reconciliation(conn, tbl, staging, [{"id": 1}], ("id",))
        assert stats == {"fetched": 1, "inserted": 0, "updated": 0, "touched": 1, "closed": 1}
        current = current_rows(conn, tbl)
        assert len(current) == 1
        assert current[0]["_natural_key"] == "[1]"


def test_composite_natural_key(table):
    engine, tbl, staging = table
    rows = [{"ambito": "M", "id": 1}, {"ambito": "N", "id": 1}]
    with engine.begin() as conn:
        stats = apply_full_reconciliation(conn, tbl, staging, rows, ("ambito", "id"))
        assert stats["inserted"] == 2
        assert len(current_rows(conn, tbl)) == 2


def test_incremental_never_closes_out_absent_keys(table):
    """A reg-date window is a subset of the table -- absence isn't deletion."""
    engine, tbl, staging = table
    with engine.begin() as conn:
        apply_full_reconciliation(conn, tbl, staging, [{"id": 1}, {"id": 2}], ("id",))
    with engine.begin() as conn:
        stats = apply_incremental(conn, tbl, staging, [{"id": 1}], ("id",))
        assert stats == {"fetched": 1, "inserted": 0, "updated": 0, "touched": 1}
        assert len(current_rows(conn, tbl)) == 2  # id=2 untouched, not closed


def test_incremental_inserts_and_versions(table):
    engine, tbl, staging = table
    with engine.begin() as conn:
        stats = apply_incremental(conn, tbl, staging, [{"id": 1, "v": "a"}], ("id",))
        assert stats == {"fetched": 1, "inserted": 1, "updated": 0, "touched": 0}
    with engine.begin() as conn:
        stats = apply_incremental(conn, tbl, staging, [{"id": 1, "v": "b"}], ("id",))
        assert stats == {"fetched": 1, "inserted": 0, "updated": 1, "touched": 0}
        current = current_rows(conn, tbl)
        assert len(current) == 1
        assert current[0]["payload"]["v"] == "b"


def test_incremental_respects_chunk_size_across_batches(table):
    engine, tbl, staging = table
    rows = [{"id": i} for i in range(10)]
    with engine.begin() as conn:
        stats = apply_incremental(conn, tbl, staging, rows, ("id",), chunk_size=3)
        assert stats == {"fetched": 10, "inserted": 10, "updated": 0, "touched": 0}
        assert len(current_rows(conn, tbl)) == 10
