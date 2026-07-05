# BDNS Sync

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

[🇪🇸 Spanish version](./README.md)

Sync engine that keeps a versioned (SCD2) copy of the [BDNS REST API](https://www.infosubvenciones.es/bdnstrans/api) in the database of your choice. Together with [`bdns-fetch`](https://github.com/cruzlorite/bdns-fetch) (extraction) they're the same BDNS project: `bdns-fetch` talks to the API, `bdns-sync` versions what it brings back.

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
bdns-sync sync concesiones_busqueda --window daily
```

Via cron, one line:

```
0 2 * * * BDNS_SYNC_TARGET_URL=bigquery://project/dataset /path/to/repo/scripts/cron_dispatch.sh
```

## Data model

Every table shares the same generic schema -- no per-endpoint fields:

`_natural_key` · `_row_hash` · `_valid_from` / `_valid_to` · `_is_current` · `payload` (JSON, full record)

If the API adds or drops a field, no migration needed: caught by hash, versioned like any other change.

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

## Good practices followed (official)

Per the official ["Buenas prácticas API SNPSAP"](https://www.infosubvenciones.es/bdnstrans/estaticos/ayuda/Buenas%20pr%C3%A1cticas%20API%20SNPSAP.pdf):

- **10 requests/second per IP limit**, enforced by `bdns-fetch`.
- **Max page size** (10,000 records/call).
- **Daily/weekly/monthly/annual cadence by registration date**: official recommendation, not our own design.
- **`terceros` unused**: the document itself flags it as redundant.
- **Full reconciliation to catch removals**: grants are withdrawn from BDNS after 4-5 years; full-catalog syncs detect that, incremental passes don't.

## Known limitations

- `organos_codigo` / `organos_codigoadmin` not implemented (group H).
- No periodic removal-detection for the big search endpoints.
- Individual records can come back malformed (confirmed live); skipped with a log instead of breaking the batch.
- No historical backfill for `convocatorias` (each record is a real API call).

## Testing

Beyond the per-module unit tests (`tests/test_scd2.py`, `tests/test_generic.py`, `tests/test_hashing.py`...), there's a **multi-day scenario suite** (`tests/test_timeline_*.py`) that simulates, for **all 22 registered entities** (17 `full` + 4 `search` + `convocatorias`), the real sequence of cron runs over time: initial load, no-op resyncs, corrections, deletions, and new arrivals.

### Data: real, not invented

The fixtures (`tests/fixtures/*.json`) are real captures from the live API, with names, NIF/CIF, and identifiers replaced by fake values (anonymized wherever it could re-identify a beneficiary -- calls, catalogs, and public org charts are left as-is, they aren't personal data). Field shape is exactly the real one.

### How the API is substituted: `FakeBDNSClient`

`bdns.sync.syncers` never imports or type-checks against `BDNSClient` -- every `sync_*` function just calls `fetch_*` methods on whatever `client` object it's given. That makes any plain Python object exposing those same methods a complete substitute -- no mocking library needed. `tests/fake_client.py` defines `FakeBDNSClient`, loaded from the anonymized fixtures and mutable between simulated "days" (`client.sectores[0]["descripcion"] = "..."`, `client.concesiones_busqueda.pop()`, ...) to script each scenario.

For incremental entities, every fixture record carries a `reg_days_ago` (days before yesterday, not an absolute date -- so the fixture never goes stale), which `FakeBDNSClient` uses to filter by window exactly as the real API would with `fechaRegInicio`/`fechaRegFin`.

### What's tested, by sync shape

| Shape | Entities | Scenarios covered |
|---|---|---|
| **Simple catalog** (`test_timeline_full_catalogs.py`) | `sectores`, `actividades`, `finalidades`, `beneficiarios`, `instrumentos`, `objetivos`, `convocatorias_ultimas`, `regiones`, `sanciones_busqueda`, `grandesbeneficiarios_anios`, `planesestrategicos_busqueda` | Initial insert · no-op resync (touch only) · field rewrite (SCD2: old version closed, new one opened) · deletion detected via full reconciliation · new record insert |
| **Swept** (`test_timeline_swept_catalogs.py`) | `organos`, `organos_agrupacion`, `reglamentos` | Same 5 scenarios above, plus: *every* declared sweep value (`idAdmon`/`ambito`) is actually requested, and populating a previously-empty sweep value never closes out another value's rows |
| **Incremental window** (`test_timeline_incremental_windows.py`) | `concesiones_busqueda`, `ayudasestado_busqueda`, `minimis_busqueda`, `partidospoliticos_busqueda` | The `daily→weekly→monthly→annual` cascade progressively reveals older registrations · a correction to a 20-day-old record is missed by `daily` and `weekly`, caught by `monthly` · deletions are never detected (a window is a subset, not the full state) · new record insert · each window's date bounds match `WINDOWS` exactly |
| **Discover-then-detail** (`test_timeline_convocatorias.py`) | `convocatorias` | Same window cascade · exactly one detail call per discovered code · detail rewrite · a malformed record is skipped without breaking the batch · deletions never detected · new arrival |
| **Multi-table** (`test_timeline_multitable.py`) | `grandesbeneficiarios_busqueda`, `planesestrategicos`/`_vigencia` | The `anios` sweep is recomputed dynamically from the catalog (not hardcoded) · full reconciliation (insert/touch/rewrite/delete) over the discovered `idPES` set · a malformed record is skipped without breaking the batch · `_vigencia` reconciles as an independent table |

These modules, together with `test_syncers.py`/`test_syncers_wiring.py` (a smoke test that all 22 entities are registered and callable), leave `scd2.py`, `generic.py`, `syncers.py`, `hashing.py`, and `schema.py` at 100% coverage.

## Development

```bash
poetry install
poetry run bdns-sync --help
make test
```

## License and links

- [GNU GPL v3.0](./LICENSE)
- [Official API](https://www.infosubvenciones.es/bdnstrans/api) · [BDNS Portal](https://www.infosubvenciones.es)
- Sibling project: [bdns-fetch](https://github.com/cruzlorite/bdns-fetch) (extraction)
