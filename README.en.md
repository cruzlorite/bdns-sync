# BDNS Sync

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

[🇪🇸 Spanish version](./README.md)

Sync engine that keeps a versioned (SCD2) copy of the [BDNS REST API](https://www.infosubvenciones.es/bdnstrans/api).

[`bdns-fetch`](https://github.com/cruzlorite/bdns-fetch) implements an interface for extracting the data, `bdns-sync` provides a storage layer on top of `bdns-fetch`.

A pure tool: one endpoint per invocation, no config file. Cadence lives in [`scripts/cron_dispatch.sh`](scripts/cron_dispatch.sh).

## Installation

```bash
git clone https://github.com/cruzlorite/bdns-sync.git
cd bdns-sync
poetry install
```

## Usage

```bash
export BDNS_SYNC_TARGET_URL="bigquery://project/dataset"   # or postgresql://..., sqlite:///...

bdns-sync list --kind full              # full-replace entities
bdns-sync list --kind search            # incremental entities
bdns-sync sync sectores                 # sync one entity
bdns-sync sync concesiones_busqueda --window daily           # cascade window
bdns-sync sync concesiones_busqueda --since 2020-01-01        # historical backfill (through yesterday)
bdns-sync sync concesiones_busqueda --since 2020-01-01 --until 2020-12-31
```

Via cron, one line (daily/weekly/monthly/annual cadence):

```
0 2 * * * BDNS_SYNC_TARGET_URL=bigquery://project/dataset /path/to/repo/scripts/cron_dispatch.sh
```

Initial historical load (once, before starting the cron):

```
BDNS_SYNC_TARGET_URL=bigquery://project/dataset /path/to/repo/scripts/full_load.sh
```

## Data model

Each synced endpoint has its own table, and they all share the same generic schema -- no per-endpoint fields. The original record goes into `payload` whole; everything else is SCD2 control columns:

| Column | What for |
|---|---|
| `_id` | Autoincrement PK, internal only |
| `_natural_key` | The record's business key (JSON of the key fields; see the entity tables below) |
| `_row_hash` | SHA-256 of the canonical payload, so a change is detected without comparing field by field |
| `_valid_from` / `_valid_to` | This version's validity span. `_valid_to` is `NULL` while it's the current version |
| `_is_current` | `True` on the live version of each natural key |
| `_synced_at` | Last time this version was seen at the source (bumped even when nothing changed) |
| `_reg_date` | The payload's own registration date. Only populated for entities with window-scoped deletion detection (see below); `NULL` for the rest |
| `payload` | JSON with the full record exactly as the API returns it |

If the API adds or drops a field, no migration needed: caught by hash, versioned like any other change.

**Control tables** (shared across all endpoints, `_sync_` prefix):

- **`_sync_state`** -- one row per table, the watermark: `table_name`, `last_synced_at`, `last_run_id`.
- **`_sync_runs`** -- append-only log of every run: `run_id`, `table_name`, `run_type` (`full` or `daily`/`weekly`/`monthly`/`annual`), `started_at`/`finished_at`, `status` (`running`/`success`/`failed`), `error`, and the counters `rows_fetched`, `rows_inserted`, `rows_soft_deleted`, `rows_skipped`.
- **`_sync_errors`** -- one row per skipped malformed record: `error_id`, `run_id`, `table_name`, `context`, `content` (truncated to 200 chars), `occurred_at`. See [Known limitations](#known-limitations).

## Endpoint types

Two families, driven by volume.

### Full replace (`bdns-sync sync <entity>`)

Small catalogs -- fetching everything every time is cheap.

| Shape | Why | Entities |
|---|---|---|
| Simple | One call, no params | `sectores`, `actividades`, `finalidades`, `beneficiarios`, `instrumentos`, `objetivos`, `convocatorias_ultimas`, `regiones` |
| Swept | The API doesn't return the union if you omit the param -- has to request each value and merge into one table | `organos`/`organos_agrupacion` (sweep `idAdmon`), `reglamentos` (sweeps `ambito`), `sanciones_busqueda` |
| Discover-then-detail | The listing doesn't carry every field | `planesestrategicos_busqueda`/`planesestrategicos`/`planesestrategicos_vigencia`, `grandesbeneficiarios_anios`/`grandesbeneficiarios_busqueda` |

### Registration-date incremental (`bdns-sync sync <entity> --window {daily,weekly,monthly,annual}`)

Tens of millions of rows -- fetching them whole every time isn't viable.

| Entity | Natural key |
|---|---|
| `concesiones_busqueda` | `id` |
| `ayudasestado_busqueda` | `idConcesion` |
| `minimis_busqueda` | `idConcesion` |
| `partidospoliticos_busqueda` | `id` |
| `convocatorias` | `codigoBDNS` |

Registration date doesn't change when a record is edited, so re-querying the same window later won't find new additions, but will catch edits via hash. Corrections cluster near registration time and taper off with age, hence the cascade: `daily` always runs, `weekly`/`monthly`/`annual` are *extra* checks the same day, not replacements for it.

#### How date windows are queried (real API behavior)

This part gathers several findings confirmed live against the API. They're documented in detail because they're subtle, absent from (or contradicted by) the official docs, and getting them wrong silently loses data.

**1. A window is a day range inclusive on both ends.** `daily` for day `X` means "registrations from `X`". The upper end of every window is *yesterday* (`date.today() - 1`), because "today"'s data isn't final until the next morning.

**2. The API's upper bound has OPPOSITE semantics depending on the endpoint.** There are two families of date parameters, and they behave inversely:

| Family | Parameters | Endpoints | Upper bound | Live check (day `D`) |
|---|---|---|---|---|
| Registration-date search | `fechaRegInicio` / `fechaRegFin` | `concesiones_busqueda`, `ayudasestado_busqueda`, `minimis_busqueda`, `partidospoliticos_busqueda` | **exclusive** (excludes day `D`) | `fechaRegFin=D` → ~0 rows for `D`; `fechaRegFin=D+1` → full day `D` (concesiones: 1 vs 58,488) |
| Convocatorias discovery | `fechaDesde` / `fechaHasta` | `convocatorias` (discovery step) | **inclusive** (includes day `D`) | `fechaHasta=D` → every convocatoria with `fechaRecepcion == D`; `fechaHasta=D+1` → days `D` and `D+1` |

The bridge from our inclusive convention to the exclusive family lives in one named place, `generic.to_api_upper_bound(inclusive_end)`, which adds a day. Convocatorias does **not** use it: its `fechaHasta` is already inclusive, so adding a day would pull in convocatorias from outside the window. Had this gone unnoticed: `daily` for the four `fechaReg` endpoints would fetch almost nothing, and every wider window would drop its most recent day (and once chunked, one day per chunk boundary -- a 28-day range chunked by day returned 8 rows instead of ~1.2M).

**3. Every window is split into pieces of at most 7 days before being fetched** (`generic.iter_date_chunks`). `daily`/`weekly` already fit in one piece, unchanged; `monthly`/`annual` get chunked. Two reasons, both confirmed live against `concesiones_busqueda`:

- **Reliability**: a 4-year range (27.4M rows) returns `ERR_MANTENIMIENTO_BBDD` intermittently at any page depth; a one-week window over the same dates didn't fail once in 6 tries.
- **Speed**: a single 30-day call took 286.7s; the same range chunked into weeks took 142.5s, both with zero errors.

Because the date bridge is applied per piece, **the result doesn't depend on chunk size**: a 14-day `partidospoliticos_busqueda` range returns exactly the same 36 rows chunked into 1-, 7-, or 14-day pieces (confirmed live). The 7-day size isn't speed-critical either: measuring a fixed 14-day `concesiones` range (~530k rows), 1/3/7/14-day chunks took 51/41/47/57s -- differences within live-load noise, zero errors at any size. 7 days is the balance kept: fast, reliable, and it lines up with the weekly window.

**4. Belt-and-suspenders check of the boundaries.** To rule out both an overlap (fetching one day too many) and a gap (dropping a day), it was confirmed live, across all 5 entities, that two adjacent days `X` and `X+1` -- fetched through the real production path with the correct per-family bridge -- satisfy: `fetch(X)` and `fetch(X+1)` are disjoint (zero overlap), and their union is exactly `fetch([X, X+1])`. The counts add up to the row: for `concesiones`, 115,862 + 68,457 = 184,319 rows, no overlap. One extra `+1` (on the exclusive family) or a wrong bound (on the inclusive one) would have double-counted the boundary day; a dropped day would have broken the union. The invariant is locked in as a permanent test (`test_generic.py`).

**Window-scoped deletion detection**: `concesiones_busqueda`, `ayudasestado_busqueda`, `minimis_busqueda`, and `convocatorias` also detect real deletions, by comparing, within the same run, what was fetched against the table rows whose own registration date (`fechaAlta`/`fechaRegistro`/`fechaRecepcion`, depending on the entity) falls in that same range. This is never compared against the previous run, since that would produce constant false positives: every row eventually ages out of a rolling window regardless of deletion. `partidospoliticos_busqueda` is excluded: confirmed live that its payload never exposes a registration-date field, despite the official doc claiming it "works the same, same filters and results" as `concesiones_busqueda`. It doesn't. This is a real, permanent limitation unless the API changes.

#### Historical load

The cascade windows only reach 365 days back (`annual`). For a full historical load, use `--since DATE [--until DATE]`, which syncs an explicit date range through exactly the same machinery (7-day chunking, per-family date bridge, deletion detection scoped to the range). How far back a full load goes is set by what the API retains per endpoint -- measured live:

| Entity | Data goes back | Bounded by |
|---|---|---|
| `concesiones_busqueda` | ~4 years | 4 calendar-year retention |
| `partidospoliticos_busqueda` | ~4 years | (tracks concesiones) |
| `ayudasestado_busqueda` | ~9-10 years | 10-year retention |
| `minimis_busqueda` | ~10 years | 10-year retention |
| `convocatorias` | ~12 years | portal start (~2014) |

These dates are **not** in the code: `bdns-sync` is a pure primitive, it doesn't know how much history each endpoint has. Just as `cron_dispatch.sh` owns the cadence, [`scripts/full_load.sh`](scripts/full_load.sh) owns the start dates and passes them via `--since`. Asking for dates before the retention just returns empty weeks (one cheap call each), so the script's dates are conservative floors, not exact firsts. The load is idempotent: re-running it duplicates nothing (SCD2 just touches already-synced records).

## Good practices followed (official)

Per the official ["Buenas prácticas API SNPSAP"](https://www.infosubvenciones.es/bdnstrans/estaticos/ayuda/Buenas%20pr%C3%A1cticas%20API%20SNPSAP.pdf):

- **10 requests/second per IP limit**, enforced by `bdns-fetch`.
- **Max page size** (10,000 records/call).
- **Daily/weekly/monthly/annual cadence by registration date**: official recommendation, not our own design.
- **`terceros` unused**: the document itself flags it as redundant.
- **Full reconciliation to catch removals**: grants are withdrawn from BDNS the 4 calendar years following the grant; full-catalog syncs detect that by comparing against the entire current state. For the big incremental endpoints, where comparing against 20M+ rows every run isn't viable, a registration-date-scoped comparison is used instead (see above).

## Known limitations

- `organos_codigo` / `organos_codigoadmin` not implemented (group H).
- `partidospoliticos_busqueda` has no deletion detection: unlike the other 4 incremental entities, its payload never exposes a registration-date field (confirmed live with 70+ real rows across two different date ranges).
- Individual malformed records are skipped with a log (WARNING level), counted in `_sync_runs.rows_skipped`, and recorded in `_sync_errors` (context plus content truncated to 200 characters, linked by `run_id`). They're never stored in the synced tables: a malformed record has no real natural key, so it can't be versioned like a normal row.
- A multi-year range requested in **one call** fails intermittently: tested live (4 years, `concesiones_busqueda`, 27.4M rows), the API returns `ERR_MANTENIMIENTO_BBDD` at any page depth, while a one-week window over the same dates didn't fail once in 6 tries. That's why everything is chunked to 7 days; the historical load (`--since`, see [Historical load](#historical-load)) inherits that chunking, so there's no need to request wide ranges by hand.

## Development

```bash
poetry install
poetry run bdns-sync --help
make test
```

## Legal disclaimer

Unofficial project, not affiliated with the Base de Datos Nacional de Subvenciones (BDNS) or Spain's Ministry of Finance. Distributed under the GPL v3, which expressly disclaims any warranty: use it at your own risk, with no warranty of any kind, and no liability accepted by the author for damages, data loss, or misuse.

The synced data comes from the [Sistema Nacional de Publicidad de Subvenciones y Ayudas Públicas](https://www.infosubvenciones.es) and is subject to its own [legal notice](https://www.infosubvenciones.es/bdnstrans/GE/es/avisolegal) and [API good-practices document](https://www.infosubvenciones.es/bdnstrans/estaticos/ayuda/Buenas%20pr%C3%A1cticas%20API%20SNPSAP.pdf). Check them before redistributing anything extracted.

## License and links

- [GNU GPL v3.0](./LICENSE)
- [Official API](https://www.infosubvenciones.es/bdnstrans/api) · [BDNS Portal](https://www.infosubvenciones.es) · [BDNS legal notice](https://www.infosubvenciones.es/bdnstrans/GE/es/avisolegal)
- Sibling project: [bdns-fetch](https://github.com/cruzlorite/bdns-fetch) (extraction)
