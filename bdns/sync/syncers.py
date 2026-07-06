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

"""One named `sync_*` function per synced entity (per docs/sync-strategy.md).
Most are a one-liner delegating to the shared runners in `bdns.sync.generic`.
convocatorias, grandesbeneficiarios, and planesestrategicos have real
multi-step logic (discovery plus per-code detail calls), so they're longer,
but they're still just functions in this same file. None of them needs its
own module.
"""

import logging
from datetime import date, timedelta
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from bdns.fetch import BDNSClient
from bdns.sync.bookkeeping import run_with_bookkeeping
from bdns.sync.generic import (
    WINDOWS,
    iter_date_chunks,
    sync_full_catalog,
    sync_search_window,
    sync_swept_catalog,
)
from bdns.sync.scd2 import apply_full_reconciliation, apply_incremental

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

ADMIN_TYPES: Tuple[str, ...] = ("C", "A", "L", "O")
REGLAMENTOS_AMBITOS: Tuple[str, ...] = ("C", "A", "M", "S", "P", "G")


def _skip_malformed(
    items: Iterator[Any], context: str, errors: Optional[List[Dict[str, str]]] = None
) -> Iterator[dict]:
    """Individual records can come back malformed. Confirmed live: BDNS's
    backend sometimes rejects a specific record with an HTML error page
    instead of JSON, for reasons outside our control (not a rate limit, not
    a params issue, since other calls immediately before or after the same
    record succeed fine). Skip and log rather than crashing the whole batch
    over one bad record.

    When `errors` is given, each skip appends a `{"context", "content"}`
    dict to it. The caller threads that list back through `stats` so
    `run_with_bookkeeping` can persist it to `_sync_errors`, a durable
    record that survives after the log scrolls away.
    """
    for item in items:
        if not isinstance(item, dict):
            content = str(item)[:200]
            logger.warning("skipping malformed record (%s): %r", context, content)
            if errors is not None:
                errors.append({"context": context, "content": content})
            continue
        yield item


# --- full-replace-every-run entities (single call, natural key `id`) ------


def sync_sectores(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(engine, client, "sectores", "fetch_sectores", ("id",))


def sync_actividades(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(engine, client, "actividades", "fetch_actividades", ("id",))


def sync_finalidades(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(engine, client, "finalidades", "fetch_finalidades", ("id",))


def sync_beneficiarios(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(engine, client, "beneficiarios", "fetch_beneficiarios", ("id",))


def sync_instrumentos(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(engine, client, "instrumentos", "fetch_instrumentos", ("id",))


def sync_objetivos(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(engine, client, "objetivos", "fetch_objetivos", ("id",))


def sync_convocatorias_ultimas(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(
        engine, client, "convocatorias_ultimas", "fetch_convocatorias_ultimas", ("id",)
    )


def sync_regiones(engine, client: BDNSClient) -> Dict[str, int]:
    # Tree-shaped, but still a single call. Unlike organos*, there's no idAdmon sweep here.
    return sync_full_catalog(engine, client, "regiones", "fetch_regiones", ("id",))


def sync_sanciones_busqueda(engine, client: BDNSClient) -> Dict[str, int]:
    # There's no real id field in the source, so this is a best-effort composite of 3 fields.
    return sync_full_catalog(
        engine,
        client,
        "sanciones_busqueda",
        "fetch_sanciones_busqueda",
        ("numeroConvocatoria", "sancionado", "fechaSancion"),
    )


# --- swept entities (sweep a param, merge into one table before diffing) --


def sync_organos(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_swept_catalog(
        engine, client, "organos", "fetch_organos", "idAdmon", ADMIN_TYPES, ("idAdmon", "id")
    )


def sync_organos_agrupacion(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_swept_catalog(
        engine,
        client,
        "organos_agrupacion",
        "fetch_organos_agrupacion",
        "idAdmon",
        ADMIN_TYPES,
        ("idAdmon", "id"),
    )


def sync_reglamentos(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_swept_catalog(
        engine,
        client,
        "reglamentos",
        "fetch_reglamentos",
        "ambito",
        REGLAMENTOS_AMBITOS,
        ("ambito", "id"),
    )


# --- big search entities (reg-date incremental, cascading windows) -------


def sync_concesiones_busqueda(engine, client: BDNSClient, window: str) -> Dict[str, int]:
    # `fechaAlta` is the payload's own registration date, confirmed live
    # against 500+ rows across two date ranges. Passing it here lets this
    # window detect real deletions, not just inserts and edits. See
    # scd2.apply_incremental for how that comparison works.
    return sync_search_window(
        engine,
        client,
        "concesiones_busqueda",
        "fetch_concesiones_busqueda",
        ("id",),
        window,
        reg_date_field="fechaAlta",
    )


def sync_ayudasestado_busqueda(engine, client: BDNSClient, window: str) -> Dict[str, int]:
    return sync_search_window(
        engine,
        client,
        "ayudasestado_busqueda",
        "fetch_ayudasestado_busqueda",
        ("idConcesion",),
        window,
        reg_date_field="fechaAlta",
    )


def sync_minimis_busqueda(engine, client: BDNSClient, window: str) -> Dict[str, int]:
    return sync_search_window(
        engine,
        client,
        "minimis_busqueda",
        "fetch_minimis_busqueda",
        ("idConcesion",),
        window,
        reg_date_field="fechaRegistro",
    )


def sync_partidospoliticos_busqueda(engine, client: BDNSClient, window: str) -> Dict[str, int]:
    # No `reg_date_field` here. Confirmed live, using 71 rows across two
    # date ranges six months apart, that this payload never carries a
    # registration-date field. The official doc claims this endpoint
    # "works the same, same filters and results" as concesiones_busqueda,
    # but that claim doesn't hold for this field. Window-scoped deletion
    # detection isn't possible here; this is a real, permanent limitation,
    # documented in the README.
    return sync_search_window(
        engine,
        client,
        "partidospoliticos_busqueda",
        "fetch_partidospoliticos_busqueda",
        ("id",),
        window,
    )


# --- convocatorias: two-step discover-then-detail -------------------------
#
# `convocatorias_busqueda` is discovery only, not stored as its own table.
# It just yields `numeroConvocatoria` codes for a window. Each code then
# costs one real detail call (`fetch_convocatorias(numConv=X)`, per the
# official doc: "aqui cada Convocatoria te costara una llamada"). That
# detail record is what actually gets versioned into the `convocatorias`
# table.
#
# Note: the search result's identifier field is `numeroConvocatoria`, but
# the detail record (what actually gets stored) carries the same value
# under a different key, `codigoBDNS`. Confirmed live; not documented
# anywhere.

CONVOCATORIAS_ENDPOINT = "convocatorias"
CONVOCATORIAS_KEY_FIELDS = ("codigoBDNS",)


def discover_convocatoria_codes(client: BDNSClient, start: date, end: date) -> Set[str]:
    """Find every `numeroConvocatoria` registered in [start, end] (inclusive).

    Chunked into `CHUNK_DAYS`-wide pieces like every other reg-date fetch
    (see `generic.iter_date_chunks`), but note the crucial difference from
    the four `fechaRegFin` search endpoints: convocatorias' `fechaHasta`
    upper bound is INCLUSIVE, not exclusive. Confirmed live -- `fechaHasta=D`
    returns every convocatoria with `fechaRecepcion == D`, the whole day. So
    `chunk_end` is sent as-is; it must NOT go through `to_api_upper_bound`
    (that would over-fetch one extra day and pull convocatorias registered
    outside the requested window).
    """
    codes: Set[str] = set()
    for chunk_start, chunk_end in iter_date_chunks(start, end):
        for item in client.fetch_convocatorias_busqueda(
            fechaDesde=chunk_start, fechaHasta=chunk_end
        ):
            codes.add(item["numeroConvocatoria"])
    return codes


def fetch_convocatoria_details(
    client: BDNSClient, codes: Set[str], errors: Optional[List[Dict[str, str]]] = None
) -> Iterator[dict]:
    """One real API call per code. This is the costly step in the two-step
    discover-then-detail flow.
    """
    for code in codes:
        yield from _skip_malformed(
            client.fetch_convocatorias(numConv=code), f"convocatorias numConv={code}", errors
        )


def sync_convocatorias(engine, client: BDNSClient, window: str) -> Dict[str, int]:
    """Two-step: discover codes for the window's reg-date-equivalent range
    (`fechaDesde/Hasta`), then fetch full detail per code and apply
    incrementally.
    """
    days = WINDOWS[window]
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    codes = discover_convocatoria_codes(client, start, end)
    errors: List[Dict[str, str]] = []

    def apply_fn(conn, table, staging):
        stats = apply_incremental(
            conn,
            table,
            staging,
            fetch_convocatoria_details(client, codes, errors),
            CONVOCATORIAS_KEY_FIELDS,
            # `fechaRecepcion` is the detail record's own registration date,
            # confirmed live against 1000 rows across two date ranges. This
            # enables window-scoped deletion detection, the same way as
            # concesiones_busqueda.
            reg_date_field="fechaRecepcion",
            window_start=start,
            window_end=end,
        )
        stats["skipped"] = len(errors)
        stats["_skip_details"] = errors
        return stats

    return run_with_bookkeeping(engine, CONVOCATORIAS_ENDPOINT, run_type=window, apply_fn=apply_fn)


# --- grandesbeneficiarios: two tables for one entity ----------------------
#
# `_anios` is a trivial flat catalog (the valid years to query). `_busqueda`
# is the actual data, but it needs `anios` swept dynamically from `_anios`
# first, rather than from a hardcoded year list, per the doc. That's why
# this doesn't reduce to a plain sync_full_catalog call.

GRANDESBENEFICIARIOS_ANIOS_ENDPOINT = "grandesbeneficiarios_anios"
GRANDESBENEFICIARIOS_BUSQUEDA_ENDPOINT = "grandesbeneficiarios_busqueda"
GRANDESBENEFICIARIOS_KEY_FIELDS = ("idPersona", "ejercicio")


def sync_grandesbeneficiarios_anios(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(
        engine,
        client,
        GRANDESBENEFICIARIOS_ANIOS_ENDPOINT,
        "fetch_grandesbeneficiarios_anios",
        ("id",),
    )


def sync_grandesbeneficiarios_busqueda(engine, client: BDNSClient) -> Dict[str, int]:
    anios = [item["id"] for item in client.fetch_grandesbeneficiarios_anios()]
    return run_with_bookkeeping(
        engine,
        GRANDESBENEFICIARIOS_BUSQUEDA_ENDPOINT,
        run_type="full",
        apply_fn=lambda conn, table, staging: apply_full_reconciliation(
            conn,
            table,
            staging,
            client.fetch_grandesbeneficiarios_busqueda(anios=anios),
            GRANDESBENEFICIARIOS_KEY_FIELDS,
        ),
    )


# --- planesestrategicos: three tables for one entity ----------------------
#
# `_busqueda` is a trivial full-crawl catalog (~2,000 rows) that doubles as
# the discovery step for the other two: detail and validity, each looked up
# by idPES and looped over the full discovered set every run. No cascading
# windows are needed; this volume is cheap enough (same reasoning as
# convocatorias, just smaller). Neither the detail nor the vigencia response
# echoes back idPES, confirmed live, so it's tagged onto the payload
# explicitly, the same pattern used for organos' idAdmon tag.

PLANESESTRATEGICOS_BUSQUEDA_ENDPOINT = "planesestrategicos_busqueda"
PLANESESTRATEGICOS_ENDPOINT = "planesestrategicos"
PLANESESTRATEGICOS_VIGENCIA_ENDPOINT = "planesestrategicos_vigencia"
PLANESESTRATEGICOS_KEY_FIELDS = ("idPES",)


def sync_planesestrategicos_busqueda(engine, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(
        engine,
        client,
        PLANESESTRATEGICOS_BUSQUEDA_ENDPOINT,
        "fetch_planesestrategicos_busqueda",
        ("id",),
    )


def discover_pes_ids(client: BDNSClient) -> Set[int]:
    return {item["id"] for item in client.fetch_planesestrategicos_busqueda()}


def fetch_pes_details(
    client: BDNSClient, ids: Set[int], errors: Optional[List[Dict[str, str]]] = None
) -> Iterator[dict]:
    for id_pes in ids:
        items = client.fetch_planesestrategicos(idPES=id_pes)
        for item in _skip_malformed(items, f"planesestrategicos idPES={id_pes}", errors):
            item = dict(item)
            item["idPES"] = id_pes
            yield item


def fetch_pes_vigencias(
    client: BDNSClient, ids: Set[int], errors: Optional[List[Dict[str, str]]] = None
) -> Iterator[dict]:
    for id_pes in ids:
        items = client.fetch_planesestrategicos_vigencia(idPES=id_pes)
        for item in _skip_malformed(items, f"planesestrategicos_vigencia idPES={id_pes}", errors):
            item = dict(item)
            item["idPES"] = id_pes
            yield item


def sync_planesestrategicos(engine, client: BDNSClient) -> Dict[str, int]:
    ids = discover_pes_ids(client)
    errors: List[Dict[str, str]] = []

    def apply_fn(conn, table, staging):
        stats = apply_full_reconciliation(
            conn, table, staging, fetch_pes_details(client, ids, errors), PLANESESTRATEGICOS_KEY_FIELDS
        )
        stats["skipped"] = len(errors)
        stats["_skip_details"] = errors
        return stats

    return run_with_bookkeeping(engine, PLANESESTRATEGICOS_ENDPOINT, run_type="full", apply_fn=apply_fn)


def sync_planesestrategicos_vigencia(engine, client: BDNSClient) -> Dict[str, int]:
    ids = discover_pes_ids(client)
    errors: List[Dict[str, str]] = []

    def apply_fn(conn, table, staging):
        stats = apply_full_reconciliation(
            conn, table, staging, fetch_pes_vigencias(client, ids, errors), PLANESESTRATEGICOS_KEY_FIELDS
        )
        stats["skipped"] = len(errors)
        stats["_skip_details"] = errors
        return stats

    return run_with_bookkeeping(
        engine, PLANESESTRATEGICOS_VIGENCIA_ENDPOINT, run_type="full", apply_fn=apply_fn
    )


# --- registries consumed directly by cli.py --------------------------------

# endpoint name -> sync(engine, client)
FULL_SYNCERS = {
    "sectores": sync_sectores,
    "actividades": sync_actividades,
    "finalidades": sync_finalidades,
    "beneficiarios": sync_beneficiarios,
    "instrumentos": sync_instrumentos,
    "objetivos": sync_objetivos,
    "convocatorias_ultimas": sync_convocatorias_ultimas,
    "regiones": sync_regiones,
    "sanciones_busqueda": sync_sanciones_busqueda,
    "organos": sync_organos,
    "organos_agrupacion": sync_organos_agrupacion,
    "reglamentos": sync_reglamentos,
    GRANDESBENEFICIARIOS_ANIOS_ENDPOINT: sync_grandesbeneficiarios_anios,
    GRANDESBENEFICIARIOS_BUSQUEDA_ENDPOINT: sync_grandesbeneficiarios_busqueda,
    PLANESESTRATEGICOS_BUSQUEDA_ENDPOINT: sync_planesestrategicos_busqueda,
    PLANESESTRATEGICOS_ENDPOINT: sync_planesestrategicos,
    PLANESESTRATEGICOS_VIGENCIA_ENDPOINT: sync_planesestrategicos_vigencia,
}

# endpoint name -> sync(engine, client, window)
SEARCH_SYNCERS = {
    "concesiones_busqueda": sync_concesiones_busqueda,
    "ayudasestado_busqueda": sync_ayudasestado_busqueda,
    "minimis_busqueda": sync_minimis_busqueda,
    "partidospoliticos_busqueda": sync_partidospoliticos_busqueda,
}
