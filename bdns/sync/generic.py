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
from typing import Dict, Sequence

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
    """
    fetch = getattr(client, fetch_method_name)
    days = WINDOWS[window]
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    return run_with_bookkeeping(
        engine,
        endpoint_name,
        run_type=window,
        apply_fn=lambda conn, table, staging: apply_incremental(
            conn,
            table,
            staging,
            fetch(fechaRegInicio=start, fechaRegFin=end),
            key_fields,
            reg_date_field=reg_date_field,
            window_start=start,
            window_end=end,
        ),
    )
