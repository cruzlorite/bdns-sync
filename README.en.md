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

**Window-scoped deletion detection**: `concesiones_busqueda`, `ayudasestado_busqueda`, `minimis_busqueda`, and `convocatorias` also detect real deletions, by comparing, within the same run, what was fetched against the table rows whose own registration date (`fechaAlta`/`fechaRegistro`/`fechaRecepcion`, depending on the entity) falls in that same range. This is never compared against the previous run, since that would produce constant false positives: every row eventually ages out of a rolling window regardless of deletion. `partidospoliticos_busqueda` is excluded: confirmed live that its payload never exposes a registration-date field, despite the official doc claiming it "works the same, same filters and results" as `concesiones_busqueda`. It doesn't. This is a real, permanent limitation unless the API changes.

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
- No historical backfill for `convocatorias` (each record is a real API call).

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
