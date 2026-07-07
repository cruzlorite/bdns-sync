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

"""One named `sync_*` function per synced entity.
Most are a one-liner delegating to the shared runners in `bdns.sync.generic`.
convocatorias, grandesbeneficiarios, and planesestrategicos have real
multi-step logic (discovery plus per-code detail calls), so they're longer,
but they're still just functions in this same file. None of them needs its
own module.
"""

import concurrent.futures
import itertools
import logging
import threading
import time
from datetime import date
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from bdns.fetch import BDNSClient, TipoAdministracion
from bdns.fetch.types import Ambito
from bdns.sync.generic import (
    all_pages,
    iter_date_chunks,
    resolve_when,
    sync_full_catalog,
    sync_search_range,
    sync_search_range_inclusive,
    sync_swept_catalog,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Sourced from bdns-fetch's own enums rather than hand-copied, so a new
# value added upstream (a new tipo de administración or ámbito) is picked
# up automatically instead of silently missing from the sweep.
ADMIN_TYPES: Tuple[str, ...] = tuple(TipoAdministracion)
REGLAMENTOS_AMBITOS: Tuple[str, ...] = tuple(Ambito)


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
    dict to it. The caller passes that list to the sink (`skipped=`), which
    persists it to its error log, a durable record that survives after the
    log scrolls away.
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

SECTORES_ENDPOINT = "sectores"
ACTIVIDADES_ENDPOINT = "actividades"
FINALIDADES_ENDPOINT = "finalidades"
BENEFICIARIOS_ENDPOINT = "beneficiarios"
INSTRUMENTOS_ENDPOINT = "instrumentos"
OBJETIVOS_ENDPOINT = "objetivos"
CONVOCATORIAS_ULTIMAS_ENDPOINT = "convocatorias_ultimas"
REGIONES_ENDPOINT = "regiones"
SANCIONES_BUSQUEDA_ENDPOINT = "sanciones_busqueda"


def sync_sectores(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(sink, client, SECTORES_ENDPOINT, "fetch_sectores", ("id",))


def sync_actividades(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(sink, client, ACTIVIDADES_ENDPOINT, "fetch_actividades", ("id",))


def sync_finalidades(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(sink, client, FINALIDADES_ENDPOINT, "fetch_finalidades", ("id",))


def sync_beneficiarios(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(sink, client, BENEFICIARIOS_ENDPOINT, "fetch_beneficiarios", ("id",))


def sync_instrumentos(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(sink, client, INSTRUMENTOS_ENDPOINT, "fetch_instrumentos", ("id",))


def sync_objetivos(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(sink, client, OBJETIVOS_ENDPOINT, "fetch_objetivos", ("id",))


def sync_convocatorias_ultimas(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(
        sink, client, CONVOCATORIAS_ULTIMAS_ENDPOINT, "fetch_convocatorias_ultimas", ("id",)
    )


def sync_regiones(sink, client: BDNSClient) -> Dict[str, int]:
    # Tree-shaped, but still a single call. Unlike organos*, there's no idAdmon sweep here.
    return sync_full_catalog(sink, client, REGIONES_ENDPOINT, "fetch_regiones", ("id",))


def sync_sanciones_busqueda(sink, client: BDNSClient) -> Dict[str, int]:
    # There's no real id field in the source, so this is a best-effort composite of 3 fields.
    return sync_full_catalog(
        sink,
        client,
        SANCIONES_BUSQUEDA_ENDPOINT,
        "fetch_sanciones_busqueda",
        ("numeroConvocatoria", "sancionado", "fechaSancion"),
    )


# --- swept entities (sweep a param, merge into one table before diffing) --

ORGANOS_ENDPOINT = "organos"
ORGANOS_AGRUPACION_ENDPOINT = "organos_agrupacion"
REGLAMENTOS_ENDPOINT = "reglamentos"


def sync_organos(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_swept_catalog(
        sink, client, ORGANOS_ENDPOINT, "fetch_organos", "idAdmon", ADMIN_TYPES, ("idAdmon", "id")
    )


def sync_organos_agrupacion(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_swept_catalog(
        sink,
        client,
        ORGANOS_AGRUPACION_ENDPOINT,
        "fetch_organos_agrupacion",
        "idAdmon",
        ADMIN_TYPES,
        ("idAdmon", "id"),
    )


def sync_reglamentos(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_swept_catalog(
        sink,
        client,
        REGLAMENTOS_ENDPOINT,
        "fetch_reglamentos",
        "ambito",
        REGLAMENTOS_AMBITOS,
        ("ambito", "id"),
    )


# --- big search entities (reg-date incremental) --------------------------
#
# Each takes either a cascade `window` name (the daily/weekly/monthly/annual
# re-verification passes) or an explicit `since`/`until` range (a historical
# backfill). `resolve_when` collapses the two into one (start, end, run_type)
# so the engine treats a one-day window and a ten-year backfill identically.

CONCESIONES_BUSQUEDA_ENDPOINT = "concesiones_busqueda"
AYUDASESTADO_BUSQUEDA_ENDPOINT = "ayudasestado_busqueda"
MINIMIS_BUSQUEDA_ENDPOINT = "minimis_busqueda"
PARTIDOSPOLITICOS_BUSQUEDA_ENDPOINT = "partidospoliticos_busqueda"


def sync_concesiones_busqueda(sink, client: BDNSClient, window=None, *, since=None, until=None) -> Dict[str, int]:
    # `fechaAlta` is the payload's own registration date, confirmed live
    # against 500+ rows across two date ranges. Passing it here lets this
    # sync detect real deletions, not just inserts and edits. See
    # Sink.sync_window for how that comparison works.
    start, end, run_type = resolve_when(window, since, until)
    return sync_search_range(
        sink,
        client,
        CONCESIONES_BUSQUEDA_ENDPOINT,
        "fetch_concesiones_busqueda",
        ("id",),
        start,
        end,
        run_type,
        reg_date_field="fechaAlta",
    )


def sync_ayudasestado_busqueda(sink, client: BDNSClient, window=None, *, since=None, until=None) -> Dict[str, int]:
    start, end, run_type = resolve_when(window, since, until)
    return sync_search_range(
        sink,
        client,
        AYUDASESTADO_BUSQUEDA_ENDPOINT,
        "fetch_ayudasestado_busqueda",
        ("idConcesion",),
        start,
        end,
        run_type,
        reg_date_field="fechaAlta",
    )


def sync_minimis_busqueda(sink, client: BDNSClient, window=None, *, since=None, until=None) -> Dict[str, int]:
    start, end, run_type = resolve_when(window, since, until)
    return sync_search_range(
        sink,
        client,
        MINIMIS_BUSQUEDA_ENDPOINT,
        "fetch_minimis_busqueda",
        ("idConcesion",),
        start,
        end,
        run_type,
        reg_date_field="fechaRegistro",
    )


def sync_partidospoliticos_busqueda(sink, client: BDNSClient, window=None, *, since=None, until=None) -> Dict[str, int]:
    # No `reg_date_field` here. Confirmed live, using 71 rows across two
    # date ranges six months apart, that this payload never carries a
    # registration-date field. The official doc claims this endpoint
    # "works the same, same filters and results" as concesiones_busqueda,
    # but that claim doesn't hold for this field. Window-scoped deletion
    # detection isn't possible here; this is a real, permanent limitation,
    # documented in the README.
    start, end, run_type = resolve_when(window, since, until)
    return sync_search_range(
        sink,
        client,
        PARTIDOSPOLITICOS_BUSQUEDA_ENDPOINT,
        "fetch_partidospoliticos_busqueda",
        ("id",),
        start,
        end,
        run_type,
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
CONVOCATORIAS_BUSQUEDA_ENDPOINT = "convocatorias_busqueda"


def sync_convocatorias_busqueda(sink, client: BDNSClient, window=None, *, since=None, until=None) -> Dict[str, int]:
    """The discovery listing itself, stored like any other `_busqueda`
    incremental entity. Same window/reg-date machinery as `sync_convocatorias`,
    but this is a plain `sync_search_range` call, not the two-step flow:
    the listing already carries everything it needs (`numeroConvocatoria`,
    `fechaRecepcion`) in one page-paginated call, no per-code detail fetch.

    This is a separate registered endpoint with its own run in `_sync_runs`,
    not a byproduct of `sync_convocatorias`'s internal discovery step -- so
    it can be synced (and fail, and be retried) independently of the
    expensive detail phase, same as every other endpoint in this file.

    Confirmed live: `numeroConvocatoria` here equals `codigoBDNS` on the
    matching detail record (same value, both strings), so this table's
    natural key lines up with `convocatorias`'s. The listing is NOT a
    substitute for the detail table: it carries only 10 of the ~30 detail
    fields (no budget, dates, documents, instruments, etc.), and its hash
    changing (or not) says nothing about whether detail-only fields
    changed -- never use it to skip a detail fetch.

    Uses `sync_search_range_inclusive`, not `sync_search_range`: this
    endpoint's date params are `fechaDesde`/`fechaHasta` (inclusive upper
    bound), the same family `discover_convocatoria_codes` uses below --
    NOT the `fechaRegInicio`/`fechaRegFin` (exclusive) family the four big
    search endpoints use. Using the wrong helper here would silently drop
    each chunk's last day, same bug class as the original daily-window fix.
    """
    start, end, run_type = resolve_when(window, since, until)
    return sync_search_range_inclusive(
        sink,
        client,
        CONVOCATORIAS_BUSQUEDA_ENDPOINT,
        "fetch_convocatorias_busqueda",
        ("numeroConvocatoria",),
        start,
        end,
        run_type,
        reg_date_field="fechaRecepcion",
    )


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
        for item in all_pages(client.fetch_convocatorias_busqueda)(
            fechaDesde=chunk_start, fechaHasta=chunk_end
        ):
            codes.add(item["numeroConvocatoria"])
    return codes


DETAIL_WORKERS = 8

# Minimum gap between request *starts* across all detail workers. The
# official limit is 10 req/s per IP, and the client's token bucket honors
# that as an average -- but its bucket starts full, so a fresh worker pool
# fires its first requests simultaneously, and the server 429s the burst
# (confirmed live: 10 workers through the token bucket alone died on
# HTTP 429 within seconds). The same server accepts a sustained 9.8 req/s
# with zero 429s when starts are spaced (confirmed live at 100ms spacing);
# 105ms keeps a small margin under the cap.
DETAIL_SPACING_SECONDS = 0.105


def fetch_convocatoria_details(
    client: BDNSClient,
    codes: Set[str],
    errors: Optional[List[Dict[str, str]]] = None,
    max_workers: int = DETAIL_WORKERS,
) -> Iterator[dict]:
    """One real API call per code. This is the costly step in the two-step
    discover-then-detail flow.

    Calls run through a thread pool: each `numConv` response is a single
    record, so the client's page-level parallelism never engages, and a
    sequential loop is latency-bound far below the 10 req/s budget
    (measured live: 0.2-1.9 s/call depending on server load, i.e. 0.5-4.5
    req/s). Request starts are paced `DETAIL_SPACING_SECONDS` apart, which
    is what the server actually enforces (see comment above); with paced
    starts the worker count only needs to cover latency (8 workers covers
    ~0.8s of latency at full rate). Submission is windowed (2x workers) so
    a backfill of tens of thousands of codes never buffers more than a
    handful of fetched records ahead of the consumer.

    `_skip_malformed` runs in the consumer thread, keeping `errors`
    single-threaded. A hard fetch failure (after the client's own retries)
    surfaces on `.result()` and fails the run, same as the sequential loop
    did.
    """
    pace_lock = threading.Lock()
    next_start = [0.0]

    def fetch_one(code):
        with pace_lock:
            now = time.monotonic()
            wait = max(0.0, next_start[0] - now)
            next_start[0] = now + wait + DETAIL_SPACING_SECONDS
        if wait:
            time.sleep(wait)
        return code, list(client.fetch_convocatorias(numConv=code))

    codes_iter = iter(codes)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        pending = {
            executor.submit(fetch_one, code)
            for code in itertools.islice(codes_iter, max_workers * 2)
        }
        while pending:
            done, pending = concurrent.futures.wait(
                pending, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for code in itertools.islice(codes_iter, len(done)):
                pending.add(executor.submit(fetch_one, code))
            for future in done:
                code, items = future.result()
                yield from _skip_malformed(items, f"convocatorias numConv={code}", errors)


def sync_convocatorias(sink, client: BDNSClient, window=None, *, since=None, until=None) -> Dict[str, int]:
    """Two-step: discover codes for the reg-date range (`fechaDesde/Hasta`),
    then fetch full detail per code and apply incrementally.

    Takes either a cascade `window` name or an explicit `since`/`until`
    backfill range, same as the four `fechaReg` search entities.
    """
    start, end, run_type = resolve_when(window, since, until)
    codes = discover_convocatoria_codes(client, start, end)
    errors: List[Dict[str, str]] = []

    return sink.sync_window(
        CONVOCATORIAS_ENDPOINT,
        fetch_convocatoria_details(client, codes, errors),
        CONVOCATORIAS_KEY_FIELDS,
        window_start=start,
        window_end=end,
        run_type=run_type,
        # `fechaRecepcion` is the detail record's own registration date,
        # confirmed live against 1000 rows across two date ranges. This
        # enables window-scoped deletion detection, the same way as
        # concesiones_busqueda.
        reg_date_field="fechaRecepcion",
        skipped=errors,
    )


# --- grandesbeneficiarios: two tables for one entity ----------------------
#
# `_anios` is a trivial flat catalog (the valid years to query). `_busqueda`
# is the actual data, but it needs `anios` swept dynamically from `_anios`
# first, rather than from a hardcoded year list, per the doc. That's why
# this doesn't reduce to a plain sync_full_catalog call.

GRANDESBENEFICIARIOS_ANIOS_ENDPOINT = "grandesbeneficiarios_anios"
GRANDESBENEFICIARIOS_BUSQUEDA_ENDPOINT = "grandesbeneficiarios_busqueda"
GRANDESBENEFICIARIOS_KEY_FIELDS = ("idPersona", "ejercicio")


def sync_grandesbeneficiarios_anios(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(
        sink,
        client,
        GRANDESBENEFICIARIOS_ANIOS_ENDPOINT,
        "fetch_grandesbeneficiarios_anios",
        ("id",),
    )


def sync_grandesbeneficiarios_busqueda(sink, client: BDNSClient) -> Dict[str, int]:
    anios = [item["id"] for item in client.fetch_grandesbeneficiarios_anios()]
    return sink.sync_full(
        GRANDESBENEFICIARIOS_BUSQUEDA_ENDPOINT,
        all_pages(client.fetch_grandesbeneficiarios_busqueda)(anios=anios),
        GRANDESBENEFICIARIOS_KEY_FIELDS,
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


def sync_planesestrategicos_busqueda(sink, client: BDNSClient) -> Dict[str, int]:
    return sync_full_catalog(
        sink,
        client,
        PLANESESTRATEGICOS_BUSQUEDA_ENDPOINT,
        "fetch_planesestrategicos_busqueda",
        ("id",),
    )


def discover_pes_ids(client: BDNSClient) -> Set[int]:
    return {item["id"] for item in all_pages(client.fetch_planesestrategicos_busqueda)()}


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


def sync_planesestrategicos(sink, client: BDNSClient) -> Dict[str, int]:
    ids = discover_pes_ids(client)
    errors: List[Dict[str, str]] = []

    return sink.sync_full(
        PLANESESTRATEGICOS_ENDPOINT,
        fetch_pes_details(client, ids, errors),
        PLANESESTRATEGICOS_KEY_FIELDS,
        skipped=errors,
    )


def sync_planesestrategicos_vigencia(sink, client: BDNSClient) -> Dict[str, int]:
    ids = discover_pes_ids(client)
    errors: List[Dict[str, str]] = []

    return sink.sync_full(
        PLANESESTRATEGICOS_VIGENCIA_ENDPOINT,
        fetch_pes_vigencias(client, ids, errors),
        PLANESESTRATEGICOS_KEY_FIELDS,
        skipped=errors,
    )


# --- registries consumed directly by cli.py --------------------------------

# endpoint name -> sync(engine, client)
FULL_SYNCERS = {
    SECTORES_ENDPOINT: sync_sectores,
    ACTIVIDADES_ENDPOINT: sync_actividades,
    FINALIDADES_ENDPOINT: sync_finalidades,
    BENEFICIARIOS_ENDPOINT: sync_beneficiarios,
    INSTRUMENTOS_ENDPOINT: sync_instrumentos,
    OBJETIVOS_ENDPOINT: sync_objetivos,
    CONVOCATORIAS_ULTIMAS_ENDPOINT: sync_convocatorias_ultimas,
    REGIONES_ENDPOINT: sync_regiones,
    SANCIONES_BUSQUEDA_ENDPOINT: sync_sanciones_busqueda,
    ORGANOS_ENDPOINT: sync_organos,
    ORGANOS_AGRUPACION_ENDPOINT: sync_organos_agrupacion,
    REGLAMENTOS_ENDPOINT: sync_reglamentos,
    GRANDESBENEFICIARIOS_ANIOS_ENDPOINT: sync_grandesbeneficiarios_anios,
    GRANDESBENEFICIARIOS_BUSQUEDA_ENDPOINT: sync_grandesbeneficiarios_busqueda,
    PLANESESTRATEGICOS_BUSQUEDA_ENDPOINT: sync_planesestrategicos_busqueda,
    PLANESESTRATEGICOS_ENDPOINT: sync_planesestrategicos,
    PLANESESTRATEGICOS_VIGENCIA_ENDPOINT: sync_planesestrategicos_vigencia,
}

# endpoint name -> sync(sink, client, window)
SEARCH_SYNCERS = {
    CONCESIONES_BUSQUEDA_ENDPOINT: sync_concesiones_busqueda,
    AYUDASESTADO_BUSQUEDA_ENDPOINT: sync_ayudasestado_busqueda,
    MINIMIS_BUSQUEDA_ENDPOINT: sync_minimis_busqueda,
    PARTIDOSPOLITICOS_BUSQUEDA_ENDPOINT: sync_partidospoliticos_busqueda,
    CONVOCATORIAS_BUSQUEDA_ENDPOINT: sync_convocatorias_busqueda,
}
