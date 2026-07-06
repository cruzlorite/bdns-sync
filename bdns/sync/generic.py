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

"""Shared sync shapes reused by several entity modules. Each entity module
still owns its own name, key fields, and any real one-off logic. Only the
mechanical "fetch, then apply" plumbing lives here.
"""

from datetime import date, timedelta
from typing import Dict, Iterator, Sequence, Tuple

from bdns.fetch import BDNSClient
from bdns.sync.bookkeeping import run_with_bookkeeping
from bdns.sync.scd2 import apply_full_reconciliation, apply_incremental

# window name -> reg-date window size in days. Shared by every entity that
# does cascading re-verification (concesiones, ayudasestado, minimis,
# partidospoliticos, convocatorias).
WINDOWS: Dict[str, int] = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
    "annual": 365,
}

# Every reg-date fetch is chunked into pieces this wide, regardless of the
# requested window. Live-tested against concesiones_busqueda: a 7-day range
# pulled cleanly (147,856 rows, 0 errors), while a 4-year range failed
# intermittently with ERR_MANTENIMIENTO_BBDD at every page depth. `daily`
# and `weekly` are already this size or smaller, so chunking is a no-op for
# them; `monthly` and `annual` are the ones this actually protects.
CHUNK_DAYS = 7


def iter_date_chunks(start: date, end: date, chunk_days: int = CHUNK_DAYS) -> Iterator[Tuple[date, date]]:
    """Split [start, end] (inclusive) into chunks of at most `chunk_days`,
    contiguous and non-overlapping. Yields (start, end) is always at least
    one item even if `start == end`.
    """
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def to_api_upper_bound(inclusive_end: date) -> date:
    """Translate an inclusive end date (how windows and chunks are expressed
    everywhere else in this codebase) into the value the four `fechaRegFin`
    search endpoints expect.

    Those endpoints (concesiones/ayudasestado/minimis/partidospoliticos)
    treat `fechaRegFin` as EXCLUSIVE: it matches registrations strictly
    before 00:00 of the given date, so it does NOT include the date's own
    day. Confirmed live: querying `fechaRegFin=D` returned 0 rows for day D
    (concesiones leaked a single midnight-exact row), while `fechaRegFin=D+1`
    returned the whole day (~58,000 for concesiones). Left as a bare
    `fechaRegFin=end`, `daily` (start == end) fetches almost nothing, and
    every wider window silently drops its most recent day per chunk.

    So the single, named, deliberate bridge: to include every registration
    made on `inclusive_end`, ask the API for the day after. This is the only
    place that crosses from our inclusive convention to the API's half-open
    one -- keep it here, not inlined as a `+ 1` at call sites.

    IMPORTANT: this is NOT universal. convocatorias' discovery uses a
    different parameter, `fechaHasta`, which is INCLUSIVE (also confirmed
    live: `fechaHasta=D` returns the full day D). That path must NOT call
    this function -- see `syncers.discover_convocatoria_codes`.
    """
    return inclusive_end + timedelta(days=1)


def sync_full_catalog(
    engine, client: BDNSClient, endpoint_name: str, fetch_method_name: str, key_fields: Sequence[str]
) -> Dict[str, int]:
    """Fetch everything with one no-arg call, full-reconcile every run."""
    fetch = getattr(client, fetch_method_name)
    return run_with_bookkeeping(
        engine,
        endpoint_name,
        run_type="full",
        apply_fn=lambda conn, table, staging: apply_full_reconciliation(
            conn, table, staging, fetch(), key_fields
        ),
    )


def sync_swept_catalog(
    engine,
    client: BDNSClient,
    endpoint_name: str,
    fetch_method_name: str,
    sweep_param: str,
    sweep_values: Sequence[str],
    key_fields: Sequence[str],
) -> Dict[str, int]:
    """Sweep `sweep_param` across `sweep_values`, merging into one table
    before reconciling. Reconciling per sweep value would wrongly close out
    the other values' rows as "missing". The sweep value is tagged onto the
    payload under `sweep_param` since the API doesn't echo it back.
    """
    fetch = getattr(client, fetch_method_name)

    def rows():
        for value in sweep_values:
            for item in fetch(**{sweep_param: value}):
                item = dict(item)
                item[sweep_param] = value
                yield item

    return run_with_bookkeeping(
        engine,
        endpoint_name,
        run_type="full",
        apply_fn=lambda conn, table, staging: apply_full_reconciliation(
            conn, table, staging, rows(), key_fields
        ),
    )


def sync_search_window(
    engine,
    client: BDNSClient,
    endpoint_name: str,
    fetch_method_name: str,
    key_fields: Sequence[str],
    window: str,
    reg_date_field: str = None,
) -> Dict[str, int]:
    """Run one cascading re-verification window: fetch the reg-date window,
    then apply incrementally. By default this never closes out keys, since
    a window is a subset of the table, not the full current state.

    `reg_date_field` opts into window-scoped deletion detection (see
    `scd2.apply_incremental`). Only entities that actually expose their own
    registration date in the payload can use this; that was confirmed live
    per entity, not assumed.

    The fetch itself is chunked into `CHUNK_DAYS`-wide pieces (see there for
    why); `window_start`/`window_end` passed to `apply_incremental` still
    cover the full requested window, since deletion scoping cares about the
    whole window, not how it was split up to fetch it.
    """
    fetch = getattr(client, fetch_method_name)
    days = WINDOWS[window]
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)

    def rows():
        for chunk_start, chunk_end in iter_date_chunks(start, end):
            yield from fetch(
                fechaRegInicio=chunk_start, fechaRegFin=to_api_upper_bound(chunk_end)
            )

    return run_with_bookkeeping(
        engine,
        endpoint_name,
        run_type=window,
        apply_fn=lambda conn, table, staging: apply_incremental(
            conn,
            table,
            staging,
            rows(),
            key_fields,
            reg_date_field=reg_date_field,
            window_start=start,
            window_end=end,
        ),
    )
