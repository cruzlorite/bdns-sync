from datetime import date, timedelta

import pytest
from sqlalchemy import MetaData, create_engine, select

from bdns.sync.generic import sync_full_catalog, sync_search_window, sync_swept_catalog
from bdns.sync.schema import build_control_tables, build_sync_table


def current_rows(engine, name):
    metadata = MetaData()
    table = build_sync_table(name, metadata)
    with engine.begin() as conn:
        return conn.execute(
            select(table).where(table.c._is_current.is_(True))
        ).mappings().all()


# --- sync_full_catalog ----------------------------------------------------


class FakeFullClient:
    def __init__(self, rows):
        self._rows = rows

    def fetch_widgets(self):
        yield from self._rows


def test_full_catalog_writes_rows_and_run_log():
    engine = create_engine("sqlite:///:memory:")
    client = FakeFullClient([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])

    stats = sync_full_catalog(engine, client, "widgets", "fetch_widgets", ("id",))
    assert stats == {"fetched": 2, "inserted": 2, "updated": 0, "touched": 0, "closed": 0}

    metadata = MetaData()
    sync_state, sync_runs = build_control_tables(metadata)
    with engine.begin() as conn:
        assert len(current_rows(engine, "widgets")) == 2
        state_row = conn.execute(
            select(sync_state).where(sync_state.c.table_name == "widgets")
        ).mappings().one()
        assert state_row["last_run_id"] == 1
        run_row = conn.execute(select(sync_runs)).mappings().one()
        assert run_row["status"] == "success"


def test_full_catalog_second_run_detects_deletion():
    engine = create_engine("sqlite:///:memory:")
    client = FakeFullClient([{"id": 1}, {"id": 2}])
    sync_full_catalog(engine, client, "widgets", "fetch_widgets", ("id",))

    client._rows = [{"id": 1}]
    stats = sync_full_catalog(engine, client, "widgets", "fetch_widgets", ("id",))
    assert stats["closed"] == 1
    assert len(current_rows(engine, "widgets")) == 1


# --- sync_swept_catalog ----------------------------------------------------


class FakeSweptClient:
    def __init__(self, by_value):
        self._by_value = by_value

    def fetch_widgets(self, region):
        yield from self._by_value.get(region, [])


def test_swept_catalog_merges_sweep_values_and_tags_payload():
    engine = create_engine("sqlite:///:memory:")
    client = FakeSweptClient(by_value={"X": [{"id": 1}], "Y": [{"id": 1}], "Z": []})

    stats = sync_swept_catalog(
        engine, client, "widgets", "fetch_widgets", "region", ("X", "Y", "Z"), ("region", "id")
    )
    assert stats["inserted"] == 2  # (X,1) and (Y,1) are distinct entities

    rows = current_rows(engine, "widgets")
    assert {r["_natural_key"] for r in rows} == {'["X",1]', '["Y",1]'}
    payloads = {r["payload"]["region"] for r in rows}
    assert payloads == {"X", "Y"}


def test_swept_catalog_does_not_close_other_sweep_values_as_missing():
    engine = create_engine("sqlite:///:memory:")
    client = FakeSweptClient(by_value={"X": [{"id": 1}], "Y": [{"id": 2}], "Z": []})
    sync_swept_catalog(
        engine, client, "widgets", "fetch_widgets", "region", ("X", "Y", "Z"), ("region", "id")
    )

    client._by_value["X"] = [{"id": 1, "v": "changed"}]
    stats = sync_swept_catalog(
        engine, client, "widgets", "fetch_widgets", "region", ("X", "Y", "Z"), ("region", "id")
    )
    assert stats["closed"] == 0
    assert len(current_rows(engine, "widgets")) == 2


# --- sync_search_window ----------------------------------------------------


class FakeSearchClient:
    def __init__(self, rows):
        self._rows = rows

    def fetch_widgets_busqueda(self, fechaRegInicio=None, fechaRegFin=None):
        self.last_window = (fechaRegInicio, fechaRegFin)
        yield from self._rows


def test_search_window_incremental_no_deletion_detection():
    engine = create_engine("sqlite:///:memory:")
    client = FakeSearchClient([{"id": 1}, {"id": 2}])
    sync_search_window(engine, client, "widgets_busqueda", "fetch_widgets_busqueda", ("id",), "daily")

    client._rows = [{"id": 1}]
    stats = sync_search_window(
        engine, client, "widgets_busqueda", "fetch_widgets_busqueda", ("id",), "daily"
    )
    assert stats == {"fetched": 1, "inserted": 0, "updated": 0, "touched": 1}

    yesterday = date.today() - timedelta(days=1)
    assert client.last_window == (yesterday, yesterday)
    assert len(current_rows(engine, "widgets_busqueda")) == 2  # id=2 not closed


def test_search_window_rejects_unknown_window():
    engine = create_engine("sqlite:///:memory:")
    client = FakeSearchClient([])
    with pytest.raises(KeyError):
        sync_search_window(
            engine, client, "widgets_busqueda", "fetch_widgets_busqueda", ("id",), "bogus"
        )
