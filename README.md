# BDNS Sync

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

[🇬🇧 English version](./README.en.md)

Motor de sincronización que mantiene copia versionada (SCD2) de la [API REST de la BDNS](https://www.infosubvenciones.es/bdnstrans/api) en la base de datos que elijas. Junto con [`bdns-fetch`](https://github.com/cruzlorite/bdns-fetch) (el cliente/extracción) forman el mismo proyecto BDNS: `bdns-fetch` habla con la API, `bdns-sync` versiona lo que trae.

## Instalación

```bash
git clone https://github.com/cruzlorite/bdns-sync.git
cd bdns-sync
poetry install
```

## Uso

```bash
export BDNS_SYNC_TARGET_URL="bigquery://proyecto/dataset"   # o postgresql://..., sqlite:///...

bdns-sync list --kind full              # entidades de reemplazo completo
bdns-sync list --kind search            # entidades incrementales
bdns-sync sync sectores                 # sincroniza una entidad
bdns-sync sync concesiones_busqueda --window daily
```

Vía cron, una línea:

```
0 2 * * * BDNS_SYNC_TARGET_URL=bigquery://proyecto/dataset /ruta/al/repo/scripts/cron_dispatch.sh
```

## Modelo de datos

Todas las tablas comparten el mismo esquema genérico -- sin campos por endpoint:

`_natural_key` · `_row_hash` · `_valid_from` / `_valid_to` · `_is_current` · `payload` (JSON con el registro completo)

Si la API añade o quita un campo, no hace falta migración: se detecta por hash y se versiona como cualquier otro cambio.

## Tipos de endpoint

Dos familias, según volumen.

### Reemplazo completo (`bdns-sync sync <entidad>`)

Catálogos pequeños -- traer todo cada vez sale barato.

| Forma | Por qué | Entidades |
|---|---|---|
| Simple | Una llamada, sin parámetros | `sectores`, `actividades`, `finalidades`, `beneficiarios`, `instrumentos`, `objetivos`, `convocatorias_ultimas`, `regiones` |
| Barrido | La API no da la unión si omites el parámetro -- hay que pedir cada valor y fusionar en una tabla | `organos`/`organos_agrupacion` (barren `idAdmon`), `reglamentos` (barre `ambito`), `sanciones_busqueda` |
| Descubre y detalla | El listado no trae todos los campos | `planesestrategicos_busqueda`/`planesestrategicos`/`planesestrategicos_vigencia`, `grandesbeneficiarios_anios`/`grandesbeneficiarios_busqueda` |

### Incremental por fecha de registro (`bdns-sync sync <entidad> --window {daily,weekly,monthly,annual}`)

Decenas de millones de filas -- traerlos enteros cada vez es inviable.

| Entidad | Clave natural |
|---|---|
| `concesiones_busqueda` | `id` |
| `ayudasestado_busqueda` | `idConcesion` |
| `minimis_busqueda` | `idConcesion` |
| `partidospoliticos_busqueda` | `id` |
| `convocatorias` | `codigoBDNS` |

La fecha de registro no cambia al editar un registro, así que reconsultar la misma ventana más tarde no encuentra altas nuevas, pero sí detecta ediciones por hash. Las correcciones se concentran cerca del registro y bajan con la antigüedad, de ahí la cascada: `daily` siempre corre, `weekly`/`monthly`/`annual` son verificaciones *extra* el mismo día, no sustituyen a la diaria.

## Buenas prácticas (oficiales)

Según el documento oficial ["Buenas prácticas API SNPSAP"](https://www.infosubvenciones.es/bdnstrans/estaticos/ayuda/Buenas%20pr%C3%A1cticas%20API%20SNPSAP.pdf):

- **Límite de 10 peticiones/segundo por IP**, aplicado por `bdns-fetch`.
- **Paginación al máximo** (10.000 registros/llamada).
- **Cadencia diaria/semanal/mensual/anual por fecha de registro**: recomendación oficial, no diseño propio.
- **`terceros` no se usa**: el propio documento lo señala como redundante.
- **Reconciliación completa para detectar bajas**: las ayudas se retiran de la BDNS a los 4-5 años; los catálogos completos lo detectan, las pasadas incrementales no.

## Limitaciones conocidas

- `organos_codigo` / `organos_codigoadmin` sin implementar (grupo H).
- Sin reconciliación periódica de bajas para los grandes endpoints de búsqueda.
- Registros puntuales pueden venir malformados (confirmado en vivo); se omiten con log en vez de romper el lote.
- Sin backfill histórico para `convocatorias` (cada registro es una llamada real).

## Testing

Además de las pruebas unitarias de cada módulo (`tests/test_scd2.py`, `tests/test_generic.py`, `tests/test_hashing.py`...), hay una suite de **escenarios multi-día** (`tests/test_timeline_*.py`) que simula, para **las 22 entidades registradas** (17 `full` + 4 `search` + `convocatorias`), la secuencia real de ejecuciones de cron a lo largo del tiempo: alta inicial, resincronizaciones sin cambios, correcciones, bajas y altas nuevas.

### Datos: reales, no inventados

Los fixtures (`tests/fixtures/*.json`) son capturas reales del API en vivo, con nombres, NIF/CIF e identificadores sustituidos por valores ficticios (`ANONIMIZAR: sí` para todo lo que pudiera reidentificar a un beneficiario -- convocatorias, catálogos y organigramas públicos se dejan tal cual, no son datos personales). La forma exacta de los campos es la real.

### Cómo se sustituye el API: `FakeBDNSClient`

`bdns.sync.syncers` nunca importa ni comprueba el tipo de `BDNSClient` -- cada `sync_*` sólo llama a métodos `fetch_*` sobre el objeto `client` que recibe. Eso hace que un objeto Python cualquiera con esos mismos métodos sea un sustituto completo: no hace falta ninguna librería de mocking. `tests/fake_client.py` define `FakeBDNSClient`, cargado con los fixtures anonimizados y mutable entre "días" simulados (`client.sectores[0]["descripcion"] = "..."`, `client.concesiones_busqueda.pop()`, ...) para guionizar cada escenario.

Para las entidades incrementales, cada registro del fixture lleva un `reg_days_ago` (días desde ayer, no una fecha absoluta -- así el fixture no caduca) que `FakeBDNSClient` usa para filtrar por ventana exactamente como haría la API real con `fechaRegInicio`/`fechaRegFin`.

### Qué se prueba, por forma de sincronización

| Forma | Entidades | Escenarios cubiertos |
|---|---|---|
| **Catálogo simple** (`test_timeline_full_catalogs.py`) | `sectores`, `actividades`, `finalidades`, `beneficiarios`, `instrumentos`, `objetivos`, `convocatorias_ultimas`, `regiones`, `sanciones_busqueda`, `grandesbeneficiarios_anios`, `planesestrategicos_busqueda` | Alta inicial · resync sin cambios (solo *touch*) · reescritura de un campo (SCD2: cierra versión vieja, abre nueva) · baja detectada por reconciliación completa · alta de un registro nuevo |
| **Barrido** (`test_timeline_swept_catalogs.py`) | `organos`, `organos_agrupacion`, `reglamentos` | Los mismos 5 escenarios anteriores, más: se pide *todos* los valores de barrido declarados (`idAdmon`/`ambito`), y poblar un valor de barrido antes vacío no cierra los registros de otro valor |
| **Incremental por ventana** (`test_timeline_incremental_windows.py`) | `concesiones_busqueda`, `ayudasestado_busqueda`, `minimis_busqueda`, `partidospoliticos_busqueda` | La cascada `daily→weekly→monthly→annual` revela progresivamente registros más antiguos · una corrección a un registro de hace 20 días la ignoran `daily` y `weekly`, la detecta `monthly` · nunca se detectan bajas (una ventana es un subconjunto, no el estado completo) · alta de un registro nuevo · los límites de fecha de cada ventana coinciden exactamente con `WINDOWS` |
| **Descubre y detalla** (`test_timeline_convocatorias.py`) | `convocatorias` | Misma cascada de ventanas · exactamente una llamada de detalle por código descubierto · reescritura de un detalle · registro malformado se salta sin romper el lote · nunca se detectan bajas · alta nueva |
| **Multi-tabla** (`test_timeline_multitable.py`) | `grandesbeneficiarios_busqueda`, `planesestrategicos`/`_vigencia` | El barrido de `anios` se recalcula dinámicamente desde el catálogo (no hardcodeado) · reconciliación completa (alta/touch/reescritura/baja) sobre el `idPES` descubierto · registro malformado se salta sin romper el lote · `_vigencia` se reconcilia como tabla independiente |

Estos módulos, junto con `test_syncers.py`/`test_syncers_wiring.py` (smoke test de que las 22 entidades están registradas y son invocables), dejan `scd2.py`, `generic.py`, `syncers.py`, `hashing.py` y `schema.py` al 100% de cobertura.

## Desarrollo

```bash
poetry install
poetry run bdns-sync --help
make test
```

## Licencia y enlaces

- [GNU GPL v3.0](./LICENSE)
- [API oficial](https://www.infosubvenciones.es/bdnstrans/api) · [Portal BDNS](https://www.infosubvenciones.es)
- Proyecto hermano: [bdns-fetch](https://github.com/cruzlorite/bdns-fetch) (extracción)
