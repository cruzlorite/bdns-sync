# BDNS Sync

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

[🇬🇧 English version](./README.en.md)

Motor de sincronización que mantiene una copia local versionada (SCD2) de la [API REST de la Base de Datos Nacional de Subvenciones (BDNS)](https://www.infosubvenciones.es/bdnstrans/api).

Se apoya en [`bdns-fetch`](https://github.com/cruzlorite/bdns-fetch), que implementa la extracción de datos del API; `bdns-sync` añade la capa de almacenamiento: versionado histórico, detección de cambios y bajas, y registro de ejecuciones.

Es una herramienta de propósito único: cada invocación sincroniza un endpoint, sin fichero de configuración. La cadencia de ejecución vive en [`scripts/delta_load.sh`](scripts/delta_load.sh).

## Índice

- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Uso](#uso)
- [Bases de datos de destino](#bases-de-datos-de-destino)
- [Operación programada](#operación-programada)
- [Modelo de datos](#modelo-de-datos)
- [Tipos de endpoint](#tipos-de-endpoint)
- [Ventanas de fecha y carga histórica](#ventanas-de-fecha-y-carga-histórica)
- [Buenas prácticas oficiales](#buenas-prácticas-oficiales)
- [Limitaciones conocidas](#limitaciones-conocidas)
- [Desarrollo](#desarrollo)
- [Aviso legal](#aviso-legal)
- [Licencia y enlaces](#licencia-y-enlaces)

## Requisitos

- Python 3.11 a 3.14
- [Poetry](https://python-poetry.org/)
- Una base de datos compatible con SQLAlchemy como destino (ver [Bases de datos de destino](#bases-de-datos-de-destino))

## Instalación

```bash
git clone https://github.com/cruzlorite/bdns-sync.git
cd bdns-sync
poetry install
```

## Uso

El destino se configura con la variable de entorno `BDNS_SYNC_TARGET_URL` (una URL de SQLAlchemy):

```bash
export BDNS_SYNC_TARGET_URL="bigquery://proyecto/dataset"   # o postgresql://..., sqlite:///...
```

Comandos principales:

```bash
bdns-sync list --kind full                                    # lista las entidades de reemplazo completo
bdns-sync list --kind search                                  # lista las entidades incrementales
bdns-sync sync sectores                                       # sincroniza una entidad de catálogo
bdns-sync sync concesiones_busqueda --window daily            # sincronización incremental por ventana
bdns-sync sync concesiones_busqueda --since 2020-01-01        # carga histórica (hasta ayer)
bdns-sync sync concesiones_busqueda --since 2020-01-01 --until 2020-12-31
```

## Bases de datos de destino

Toda la lógica de sincronización usa SQL portable (subconsultas `EXISTS`/`NOT EXISTS` correlacionadas, sin `MERGE` ni `UPDATE ... FROM` específicos de un motor), por lo que cualquier base de datos con dialecto de SQLAlchemy sirve como destino. Verificado:

| Destino | Estado | Notas |
|---|---|---|
| SQLite | Verificado (suite de tests completa) | Sin configuración adicional |
| BigQuery | Verificado (en vivo, ciclo SCD2 completo) | Driver incluido como dependencia; ver más abajo |
| PostgreSQL / MySQL | Compatible por diseño (SQL portable) | Requieren instalar su driver (`psycopg2`, `pymysql`, ...) |

El almacenamiento está detrás de una interfaz `Sink` ([`bdns/sync/sinks/`](bdns/sync/sinks/)): la capa de fetch entrega lotes de registros y el sink es dueño de todo lo demás (versionado SCD2, detección de bajas, registro de ejecuciones). La implementación actual es [`SQLSink`](bdns/sync/sinks/sql/__init__.py), que cubre cualquier motor con dialecto de SQLAlchemy; las diferencias por motor se concentran en sus adaptadores internos ([`bdns/sync/sinks/sql/dialects.py`](bdns/sync/sinks/sql/dialects.py)). Un futuro destino no SQL (p. ej. Parquet) sería otra implementación de `Sink`, sin tocar la capa de fetch.

### BigQuery

```bash
export BDNS_SYNC_TARGET_URL="bigquery://<proyecto>/<dataset>"
```

- **Autenticación**: credenciales por defecto de la aplicación (`gcloud auth application-default login`) o cuenta de servicio vía `GOOGLE_APPLICATION_CREDENTIALS`.
- **Permisos mínimos**: `roles/bigquery.dataEditor` sobre el dataset y `roles/bigquery.jobUser` sobre el proyecto.
- **Índices**: BigQuery no tiene índices secundarios; el adaptador los omite y en su lugar las tablas se crean con `CLUSTER BY (_natural_key, _is_current)`, las columnas por las que filtra toda la maquinaria SCD2.

## Operación programada

Para la operación continua basta una línea de cron. El script `delta_load.sh` decide internamente qué entidades y ventanas ejecutar cada día (cadencia diaria, semanal, mensual y anual):

```
0 2 * * * BDNS_SYNC_TARGET_URL=bigquery://proyecto/dataset /ruta/al/repo/scripts/delta_load.sh
```

Antes de arrancar el cron, ejecute una única vez la carga histórica inicial:

```
BDNS_SYNC_TARGET_URL=bigquery://proyecto/dataset /ruta/al/repo/scripts/full_load.sh
```

La carga es idempotente: reejecutarla no duplica datos.

## Modelo de datos

Cada endpoint sincronizado tiene su propia tabla, y todas comparten el mismo esquema genérico, sin campos específicos por endpoint. El registro original se almacena íntegro en `payload`; el resto son columnas de control SCD2:

| Columna | Descripción |
|---|---|
| `_natural_key` | Clave de negocio del registro (JSON de los campos clave; ver las tablas de entidades más abajo). Junto con `_valid_from` identifica cada versión |
| `_row_hash` | SHA-256 del payload canónico; permite detectar cambios sin comparar campo a campo |
| `_valid_from` / `_valid_to` | Periodo de vigencia de esta versión. `_valid_to` es `NULL` mientras es la versión actual |
| `_is_current` | `True` en la versión vigente de cada clave natural |
| `_synced_at` | Última vez que esta versión se observó en el origen (se actualiza aunque no haya cambios) |
| `_reg_date` | Fecha de registro propia del payload. Solo se rellena en las entidades con detección de bajas por ventana; `NULL` en el resto |
| `payload` | El registro completo tal como lo devuelve el API, serializado como JSON (columna de texto, portable entre motores) |

Si el API añade o elimina un campo no se requiere migración: el cambio se detecta por hash y se versiona como cualquier otro.

### Tablas de control

Compartidas por todos los endpoints, con prefijo `_sync_`:

- **`_sync_state`**: una fila por tabla, con la marca de agua: `table_name`, `last_synced_at`, `last_run_id`.
- **`_sync_runs`**: registro *append-only* de cada ejecución: `run_id`, `table_name`, `run_type` (`full` o `daily`/`weekly`/`monthly`/`annual`), `started_at`/`finished_at`, `status` (`running`/`success`/`failed`), `error`, y los contadores `rows_fetched`, `rows_inserted`, `rows_soft_deleted` y `rows_skipped`.
- **`_sync_errors`**: una fila por registro malformado descartado: `error_id`, `run_id`, `table_name`, `context`, `content` (truncado a 200 caracteres), `occurred_at`. Ver [Limitaciones conocidas](#limitaciones-conocidas).

## Tipos de endpoint

Hay dos familias, determinadas por el volumen de datos.

### Reemplazo completo (`bdns-sync sync <entidad>`)

Catálogos pequeños, donde traer el conjunto completo en cada ejecución es asumible.

| Forma | Motivo | Entidades |
|---|---|---|
| Simple | Una sola llamada, sin parámetros | `sectores`, `actividades`, `finalidades`, `beneficiarios`, `instrumentos`, `objetivos`, `convocatorias_ultimas`, `regiones` |
| Barrido | El API no devuelve la unión si se omite el parámetro; hay que consultar cada valor y fusionar los resultados en una tabla | `organos`/`organos_agrupacion` (barren `idAdmon`), `reglamentos` (barre `ambito`), `sanciones_busqueda` |
| Descubrimiento y detalle | El listado no incluye todos los campos | `planesestrategicos_busqueda`/`planesestrategicos`/`planesestrategicos_vigencia`, `grandesbeneficiarios_anios`/`grandesbeneficiarios_busqueda` |

### Incremental por fecha de registro (`bdns-sync sync <entidad> --window {daily,weekly,monthly,annual}`)

Endpoints con decenas de millones de filas, donde el reemplazo completo no es viable.

| Entidad | Clave natural |
|---|---|
| `concesiones_busqueda` | `id` |
| `ayudasestado_busqueda` | `idConcesion` |
| `minimis_busqueda` | `idConcesion` |
| `partidospoliticos_busqueda` | `id` |
| `convocatorias_busqueda` | `numeroConvocatoria` |
| `convocatorias` | `codigoBDNS` |

`convocatorias` es un caso de dos pasos: el descubrimiento consulta el listado de `convocatorias_busqueda` por rango de fechas para obtener los códigos registrados en la ventana, y cada código descubierto se solicita después completo al endpoint de detalle (`convocatorias`, por `numConv`). El registro de detalle es el que se versiona en la tabla `convocatorias`; el listado de descubrimiento se sincroniza además como su propia tabla, `convocatorias_busqueda`, con la misma máquina incremental que el resto de entidades de esta sección.

`convocatorias_busqueda` **no sustituye** a `convocatorias`: el listado trae solo 10 de los ~30 campos del detalle (sin presupuesto, fechas de solicitud, documentos, instrumentos, etc.), y que su hash no cambie no dice nada sobre si cambió un campo exclusivo del detalle. Nunca se debe usar el listado para decidir si conviene omitir la llamada de detalle de un código.

El paso de detalle de `convocatorias` es el caro: una llamada real por código descubierto, sin paginación posible (una sola llamada trae un único registro). Medido en vivo con un mes real (mayo de 2026, 6.186 códigos), en secuencial esto tardó entre 23 minutos y 3 h 12 min según la carga del servidor. El paso de detalle está paralelizado (8 hilos, ritmo de arranque de ~9,5 peticiones/segundo, justo bajo el límite de 10/s) para acercarse al límite oficial en vez de quedar atado a la latencia de una sola conexión; el mismo mes tardó 10 min 54 s en paralelo, sin ningún `429`.

La fecha de registro de un registro no cambia cuando este se edita, por lo que reconsultar la misma ventana más adelante no encuentra altas nuevas, pero sí detecta ediciones mediante el hash. Las correcciones se concentran cerca de la fecha de registro y disminuyen con la antigüedad; de ahí la cascada de ventanas: `daily` se ejecuta siempre, y `weekly`/`monthly`/`annual` son verificaciones adicionales del mismo día, no sustitutos de la diaria.

## Ventanas de fecha y carga histórica

El manejo de fechas contra el API tiene varias sutilezas verificadas en vivo, documentadas en detalle en [docs/api-behavior.md](docs/api-behavior.md). Resumen:

- Una ventana es un rango de días inclusivo por ambos extremos; el extremo superior siempre es ayer.
- El API tiene dos familias de parámetros de fecha con semántica de extremo superior **opuesta** (exclusiva en `fechaRegFin`, inclusiva en `fechaHasta`). La conversión está centralizada en `generic.to_api_upper_bound`.
- Toda ventana se trocea en piezas de máximo 7 días antes de consultarse, por fiabilidad y velocidad. El resultado no depende del tamaño de trozo.
- La corrección de los límites (sin solapamientos ni huecos entre días consecutivos) está verificada en vivo en las 5 entidades y fijada como test permanente.
- Cuatro de las cinco entidades incrementales detectan además bajas reales, comparando lo obtenido contra las filas de la tabla cuya fecha de registro cae en el mismo rango.

Las ventanas en cascada llegan como máximo a 365 días atrás. Para la carga histórica completa se usa `--since DATE [--until DATE]`, que emplea exactamente la misma maquinaria. La profundidad histórica disponible depende de la retención del API por endpoint, desde ~4 años (`concesiones_busqueda`) hasta ~12 (`convocatorias`); [`scripts/full_load.sh`](scripts/full_load.sh) ya incluye fechas de inicio conservadoras por entidad. Ver la tabla completa en [docs/api-behavior.md](docs/api-behavior.md#6-profundidad-histórica-por-endpoint).

## Buenas prácticas oficiales

El diseño sigue el documento oficial ["Buenas prácticas API SNPSAP"](https://www.infosubvenciones.es/bdnstrans/estaticos/ayuda/Buenas%20pr%C3%A1cticas%20API%20SNPSAP.pdf):

- **Límite de 10 peticiones por segundo y por IP**, aplicado por `bdns-fetch`.
- **Paginación al tamaño máximo** (10.000 registros por llamada).
- **Cadencia diaria/semanal/mensual/anual por fecha de registro**, tal como recomienda el documento.
- **El endpoint `terceros` no se usa**: el propio documento lo señala como redundante.
- **Reconciliación para detectar bajas**: las ayudas se retiran de la BDNS a los 4 años naturales siguientes a la concesión. Los catálogos completos detectan las bajas comparando contra todo el estado actual; en los grandes endpoints incrementales, donde esa comparación no es viable, se usa una comparación acotada por fecha de registro (ver [docs/api-behavior.md](docs/api-behavior.md#5-detección-de-bajas-acotada-por-ventana)).

## Limitaciones conocidas

- `organos_codigo` y `organos_codigoadmin` no están implementados (grupo H).
- `partidospoliticos_busqueda` no tiene detección de bajas: a diferencia de las otras cuatro entidades incrementales, su payload no expone ningún campo de fecha de registro (verificado en vivo; ver [docs/api-behavior.md](docs/api-behavior.md#5-detección-de-bajas-acotada-por-ventana)).
- Los registros individuales malformados se descartan con un aviso en el log (nivel WARNING), se contabilizan en `_sync_runs.rows_skipped` y quedan registrados en `_sync_errors` (contexto y contenido truncado a 200 caracteres, enlazados por `run_id`). Nunca se almacenan en las tablas sincronizadas: sin clave natural válida no pueden versionarse como una fila normal.
- Los rangos de fechas de varios años solicitados en una sola llamada fallan de forma intermitente en el API (`ERR_MANTENIMIENTO_BBDD`). Por ello toda consulta se trocea en piezas de 7 días, incluida la carga histórica con `--since`; no es necesario trocear manualmente.
- El staging no deduplica: si el mismo registro entra dos veces en el lote de una ejecución, produce dos versiones vigentes idénticas de la misma clave natural. El API y el troceado por fechas están verificados en vivo como libres de duplicados (8.086/8.086 claves distintas en una réplica exacta del fetch de producción), así que en operación normal no ocurre; el caso observado provino de un proceso zombi escribiendo en el staging de otra ejecución. Un `SELECT DISTINCT` preventivo en la inserción de versiones está pendiente como refuerzo.

## Desarrollo

```bash
poetry install
poetry run bdns-sync --help
make test
```

## Aviso legal

Proyecto no oficial, sin afiliación con la Base de Datos Nacional de Subvenciones (BDNS) ni con el Ministerio de Hacienda. Se distribuye bajo licencia GPL v3, que excluye expresamente cualquier garantía: el uso es bajo responsabilidad del usuario, sin garantía de ningún tipo y sin que el autor asuma responsabilidad alguna por daños, pérdidas de datos o usos indebidos.

Los datos sincronizados proceden del [Sistema Nacional de Publicidad de Subvenciones y Ayudas Públicas](https://www.infosubvenciones.es) y están sujetos a su propio [aviso legal](https://www.infosubvenciones.es/bdnstrans/GE/es/avisolegal) y a las [buenas prácticas del API](https://www.infosubvenciones.es/bdnstrans/estaticos/ayuda/Buenas%20pr%C3%A1cticas%20API%20SNPSAP.pdf).

## Licencia y enlaces

- [GNU GPL v3.0](./LICENSE)
- [API oficial](https://www.infosubvenciones.es/bdnstrans/api) · [Portal BDNS](https://www.infosubvenciones.es) · [Aviso legal BDNS](https://www.infosubvenciones.es/bdnstrans/GE/es/avisolegal)
- Proyecto hermano: [bdns-fetch](https://github.com/cruzlorite/bdns-fetch) (extracción)
