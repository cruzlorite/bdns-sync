"""Multi-day scenarios for the reg-date incremental "big search" endpoints:
concesiones_busqueda, ayudasestado_busqueda, minimis_busqueda,
partidospoliticos_busqueda -- run through every cascade window
(daily/weekly/monthly/annual).

The interesting behaviour here isn't insert/touch/rewrite in isolation
(covered generically in test_scd2.py) -- it's the *cascade*: daily only
looks at yesterday, so a correction to a record registered 20 days ago is
invisible to daily and weekly, and only a monthly (or annual) pass catches
it. That's the whole reason the official BDNS guidance recommends running
all four cadences instead of just daily, and it's what these tests prove
end to end against real-shaped payloads.

Each fixture record carries `reg_days_ago` (see tests/fake_client.py):
0 -> inside daily; 5 -> inside weekly, outside daily; 20 -> inside monthly,
outside weekly; 100 -> inside annual, outside monthly.
"""

from copy import deepcopy
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine

from bdns.sync.generic import WINDOWS
from bdns.sync.syncers import SEARCH_SYNCERS
from tests.fake_client import FakeBDNSClient
from tests.timeline_helpers import current_rows

# (endpoint name, key fields) -- endpoint name doubles as the fixture file
# name, the client attribute, and the sync table name.
INCREMENTAL_CASES = [
    ("concesiones_busqueda", ("id",)),
    ("ayudasestado_busqueda", ("idConcesion",)),
    ("minimis_busqueda", ("idConcesion",)),
    ("partidospoliticos_busqueda", ("id",)),
]

CASE_IDS = [name for name, _ in INCREMENTAL_CASES]


@pytest.mark.parametrize("endpoint,key_fields", INCREMENTAL_CASES, ids=CASE_IDS)
def test_cascade_progressively_reveals_older_registrations(endpoint, key_fields):
    """Running daily->weekly->monthly->annual in sequence (same as a real
    day's cron dispatch) picks up one more reg_days_ago tier each time.
    """
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()
    sync_fn = SEARCH_SYNCERS[endpoint]

    stats = sync_fn(engine, client, "daily")
    assert stats["inserted"] == 1  # only reg_days_ago=0
    assert len(current_rows(engine, endpoint)) == 1

    stats = sync_fn(engine, client, "weekly")
    assert stats["inserted"] == 1  # reg_days_ago=5 newly in range
    assert stats["touched"] == 1  # reg_days_ago=0 re-seen, unchanged
    assert len(current_rows(engine, endpoint)) == 2

    stats = sync_fn(engine, client, "monthly")
    assert stats["inserted"] == 1  # reg_days_ago=20
    assert stats["touched"] == 2
    assert len(current_rows(engine, endpoint)) == 3

    stats = sync_fn(engine, client, "annual")
    assert stats["inserted"] == 1  # reg_days_ago=100
    assert stats["touched"] == 3
    assert len(current_rows(engine, endpoint)) == 4


@pytest.mark.parametrize("endpoint,key_fields", INCREMENTAL_CASES, ids=CASE_IDS)
def test_daily_and_weekly_miss_a_correction_only_monthly_catches(endpoint, key_fields):
    """A record registered 20 days ago gets corrected upstream. Daily
    (window=1) and weekly (window=7) never look far back enough to see it;
    monthly (window=30) does. This is the cascade's whole reason to exist.
    """
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()
    sync_fn = SEARCH_SYNCERS[endpoint]

    # seed the table with all four tiers first (one full cascade)
    for window in ("daily", "weekly", "monthly", "annual"):
        sync_fn(engine, client, window)
    assert len(current_rows(engine, endpoint)) == 4

    # upstream corrects the reg_days_ago=20 record (mutate a non-key field)
    records = getattr(client, endpoint)
    target = next(r for r in records if r["reg_days_ago"] == 20)
    payload_field = next(k for k in target["payload"] if k not in key_fields and isinstance(target["payload"][k], str))
    target["payload"][payload_field] = "__CORRECTED__"

    stats = sync_fn(engine, client, "daily")
    assert stats.get("updated", 0) == 0  # correction is 20 days back, daily can't see it

    stats = sync_fn(engine, client, "weekly")
    assert stats.get("updated", 0) == 0  # still can't see it, 20 > 7

    stats = sync_fn(engine, client, "monthly")
    assert stats["updated"] == 1  # 20 <= 30 -- caught

    current = current_rows(engine, endpoint)
    updated_row = next(r for r in current if r["payload"].get(payload_field) == "__CORRECTED__")
    assert updated_row is not None


@pytest.mark.parametrize("endpoint,key_fields", INCREMENTAL_CASES, ids=CASE_IDS)
def test_incremental_never_reports_or_applies_deletions(endpoint, key_fields):
    """A reg-date window is a subset of the table, not the full current
    state -- a record missing from one window's fetch must never be closed
    out. (Deletion detection is reserved for full-reconciliation passes,
    which these entities don't get -- see README "Limitaciones conocidas".)
    """
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()
    sync_fn = SEARCH_SYNCERS[endpoint]
    for window in ("daily", "weekly", "monthly", "annual"):
        sync_fn(engine, client, window)
    before = len(current_rows(engine, endpoint))

    records = getattr(client, endpoint)
    records.remove(next(r for r in records if r["reg_days_ago"] == 0))

    stats = sync_fn(engine, client, "daily")
    assert "closed" not in stats
    assert len(current_rows(engine, endpoint)) == before  # nothing closed


@pytest.mark.parametrize("endpoint,key_fields", INCREMENTAL_CASES, ids=CASE_IDS)
def test_new_registration_is_caught_by_the_next_daily_run(endpoint, key_fields):
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()
    sync_fn = SEARCH_SYNCERS[endpoint]
    sync_fn(engine, client, "daily")

    records = getattr(client, endpoint)
    new_record = deepcopy(next(r for r in records if r["reg_days_ago"] == 0))
    for field in key_fields:
        new_record["payload"][field] = (
            new_record["payload"][field] + 999000
            if isinstance(new_record["payload"][field], int)
            else f"{new_record['payload'][field]}-NEW"
        )
    records.append(new_record)

    stats = sync_fn(engine, client, "daily")
    assert stats["inserted"] == 1


@pytest.mark.parametrize("endpoint,key_fields", INCREMENTAL_CASES, ids=CASE_IDS)
@pytest.mark.parametrize("window,days", list(WINDOWS.items()))
def test_window_date_bounds_match_the_declared_cadence(endpoint, key_fields, window, days):
    """`--window daily/weekly/monthly/annual` must translate into the exact
    reg-date range the README documents: ending yesterday, spanning `days`
    days back -- this is the "does the CLI option actually change API
    request behaviour" check.
    """
    engine = create_engine("sqlite:///:memory:")
    client = FakeBDNSClient()
    sync_fn = SEARCH_SYNCERS[endpoint]
    sync_fn(engine, client, window)

    method = f"fetch_{endpoint}"
    [call] = client.calls_to(method)
    expected_end = date.today() - timedelta(days=1)
    expected_start = expected_end - timedelta(days=days - 1)
    assert call["end"] == expected_end
    assert call["start"] == expected_start


def test_search_syncers_registry_covers_every_incremental_entity():
    assert set(SEARCH_SYNCERS) == {name for name, _ in INCREMENTAL_CASES}
