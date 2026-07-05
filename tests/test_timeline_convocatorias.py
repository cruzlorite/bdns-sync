"""Multi-day scenarios for `convocatorias`: discover codes in a reg-date
window, then one real detail call per discovered code (per the official
doc: "aqui cada Convocatoria te costara una llamada"). Same cascade
semantics as the other incremental entities, plus the two things unique to
this shape: exactly one detail call per discovered code, and malformed
detail responses must be skipped rather than crash the run.
"""

from copy import deepcopy

from sqlalchemy import create_engine

from bdns.sync.syncers import sync_convocatorias
from tests.fake_client import FakeBDNSClient
from tests.timeline_helpers import current_rows


def test_convocatorias_cascade_progressively_reveals_older_registrations():
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()

    stats = sync_convocatorias(engine, client, "daily")
    assert stats["inserted"] == 1  # only reg_days_ago=0 -> code 927266
    assert client.calls_to("fetch_convocatorias") == [{"numConv": "927266"}]

    stats = sync_convocatorias(engine, client, "weekly")
    assert stats["inserted"] == 1  # code 927267 (reg_days_ago=5) newly in range
    assert stats["touched"] == 1  # code 927266 re-discovered, unchanged
    assert len(current_rows(engine, "convocatorias")) == 2

    stats = sync_convocatorias(engine, client, "monthly")
    assert stats["inserted"] == 1  # code 927268 (reg_days_ago=20)
    assert stats["touched"] == 2
    assert len(current_rows(engine, "convocatorias")) == 3


def test_convocatorias_one_detail_call_per_discovered_code():
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()
    sync_convocatorias(engine, client, "monthly")
    calls = client.calls_to("fetch_convocatorias")
    assert sorted(c["numConv"] for c in calls) == ["927266", "927267", "927268"]


def test_convocatorias_detail_rewrite_produces_new_version():
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()
    sync_convocatorias(engine, client, "weekly")

    client.convocatorias_detail["927267"]["presupuestoTotal"] = 999999
    stats = sync_convocatorias(engine, client, "weekly")
    assert stats["updated"] == 1
    assert stats["touched"] == 1  # 927266 unchanged

    current = current_rows(engine, "convocatorias")
    rewritten = next(r for r in current if r["_natural_key"] == '["927267"]')
    assert rewritten["payload"]["presupuestoTotal"] == 999999


def test_convocatorias_malformed_detail_is_skipped_not_crashed():
    """Live-confirmed failure mode: BDNS occasionally returns an HTML error
    page instead of JSON for one specific record. That code's detail must
    be skipped, not blow up the whole run.
    """
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()
    client.convocatorias_detail["927266"] = "<html>not json</html>"

    stats = sync_convocatorias(engine, client, "daily")
    assert stats["fetched"] == 0  # the only discovered code was malformed
    assert stats["inserted"] == 0
    assert len(current_rows(engine, "convocatorias")) == 0


def test_convocatorias_never_closes_out_codes_missing_from_window():
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()
    sync_convocatorias(engine, client, "monthly")
    before = len(current_rows(engine, "convocatorias"))

    client.convocatorias_busqueda = [
        rec for rec in client.convocatorias_busqueda if rec["reg_days_ago"] != 0
    ]
    stats = sync_convocatorias(engine, client, "daily")
    assert "closed" not in stats
    assert len(current_rows(engine, "convocatorias")) == before


def test_convocatorias_new_registration_caught_by_daily():
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()
    sync_convocatorias(engine, client, "daily")

    new_discovery = deepcopy(client.convocatorias_busqueda[0])
    new_discovery["payload"] = dict(new_discovery["payload"], numeroConvocatoria="927270")
    client.convocatorias_busqueda.append(new_discovery)
    client.convocatorias_detail["927270"] = dict(
        client.convocatorias_detail["927266"], codigoBDNS="927270"
    )

    stats = sync_convocatorias(engine, client, "daily")
    assert stats["inserted"] == 1
