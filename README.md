# BDNS Sync

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

[🇬🇧 English version](./README.en.md)

Motor de sincronización que mantiene una copia versionada (SCD2) de la [API REST de la BDNS](https://www.infosubvenciones.es/bdnstrans/api).

[`bdns-fetch`](https://github.com/cruzlorite/bdns-fetch) implementa una interfaz para extraer información, `bdns-sync` ofrece una capa de almacenamiento encima de `bdns-fetch`.

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

Cada endpoint sincronizado tiene su propia tabla, y todas comparten el mismo esquema genérico -- sin campos por endpoint. El registro original entra entero en `payload`; el resto son columnas de control SCD2:

| Columna | Para qué |
|---|---|
| `_id` | PK autoincremental, solo interna |
| `_natural_key` | Clave de negocio del registro (JSON de los campos clave; ver tabla de entidades más abajo) |
| `_row_hash` | SHA-256 del payload canónico; así se detecta un cambio sin comparar campo a campo |
| `_valid_from` / `_valid_to` | Vigencia de esta versión. `_valid_to` es `NULL` mientras es la versión actual |
| `_is_current` | `True` en la versión vigente de cada clave natural |
| `_synced_at` | Última vez que esta versión se vio en el origen (se actualiza aunque no cambie) |
| `_reg_date` | Fecha de registro propia del payload. Solo se rellena en las entidades con detección de bajas por ventana (ver más abajo); en el resto queda `NULL` |
| `payload` | JSON con el registro completo tal cual lo devuelve la API |

Si la API añade o quita un campo, no hace falta migración: se detecta por hash y se versiona como cualquier otro cambio.

**Tablas de control** (compartidas por todos los endpoints, prefijo `_sync_`):

- **`_sync_state`** -- una fila por tabla, la marca de agua: `table_name`, `last_synced_at`, `last_run_id`.
- **`_sync_runs`** -- registro append-only de cada ejecución: `run_id`, `table_name`, `run_type` (`full` o `daily`/`weekly`/`monthly`/`annual`), `started_at`/`finished_at`, `status` (`running`/`success`/`failed`), `error`, y los contadores `rows_fetched`, `rows_inserted`, `rows_soft_deleted`, `rows_skipped`.
- **`_sync_errors`** -- una fila por registro malformado descartado: `error_id`, `run_id`, `table_name`, `context`, `content` (truncado a 200 caracteres), `occurred_at`. Ver [Limitaciones conocidas](#limitaciones-conocidas).

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

#### Cómo se consultan las ventanas de fecha (comportamiento real del API)

Esta parte concentra varios hallazgos comprobados en vivo contra el API. Se documentan en detalle porque son sutiles, no están en la documentación oficial (o la contradicen), y equivocarse aquí provoca pérdida silenciosa de datos.

**1. La ventana es un rango de días inclusivo por ambos extremos.** `daily` sobre el día `X` significa "los registros de `X`". El extremo superior de toda ventana es *ayer* (`date.today() - 1`), porque los datos de "hoy" no están cerrados hasta la mañana siguiente.

**2. El extremo superior del API tiene semántica OPUESTA según el endpoint.** Hay dos familias de parámetros de fecha, y se comportan al revés:

| Familia | Parámetros | Endpoints | Extremo superior | Comprobación en vivo (día `D`) |
|---|---|---|---|---|
| Búsqueda por fecha de registro | `fechaRegInicio` / `fechaRegFin` | `concesiones_busqueda`, `ayudasestado_busqueda`, `minimis_busqueda`, `partidospoliticos_busqueda` | **exclusivo** (no incluye el día `D`) | `fechaRegFin=D` → ~0 filas de `D`; `fechaRegFin=D+1` → día `D` completo (en `concesiones`, 1 vs 58.488) |
| Descubrimiento de convocatorias | `fechaDesde` / `fechaHasta` | `convocatorias` (paso de descubrimiento) | **inclusivo** (sí incluye el día `D`) | `fechaHasta=D` → todas las convocatorias con `fechaRecepcion == D`; `fechaHasta=D+1` → días `D` y `D+1` |

El puente entre nuestra convención (inclusiva) y la familia exclusiva está en un único sitio con nombre propio, `generic.to_api_upper_bound(fin_inclusivo)`, que suma un día. Convocatorias **no** lo usa: su `fechaHasta` ya es inclusivo, sumar un día traería convocatorias de fuera de la ventana. Si esto no se hubiera detectado: `daily` de los cuatro endpoints `fechaReg` no traería casi nada, y toda ventana más ancha perdería su día más reciente (y, una vez troceada, un día por cada frontera de trozo -- un rango de 28 días troceado por día devolvía 8 filas en vez de ~1,2M).

**3. Cada ventana se trocea en piezas de máximo 7 días antes de pedirla** (`generic.iter_date_chunks`). `daily`/`weekly` ya caben en una pieza, no cambian; `monthly`/`annual` sí se trocean. Motivos, ambos comprobados en vivo contra `concesiones_busqueda`:

- **Fiabilidad**: un rango de 4 años (27,4M filas) devuelve `ERR_MANTENIMIENTO_BBDD` de forma intermitente en cualquier profundidad de página; una ventana semanal sobre las mismas fechas no falló ni una vez en 6 intentos.
- **Velocidad**: un rango de 30 días de una sola vez tardó 286,7s; el mismo rango troceado en semanas tardó 142,5s, ambos sin errores.

Como el puente de fecha se aplica por pieza, **el resultado no depende del tamaño de trozo**: un rango de 14 días de `partidospoliticos_busqueda` devuelve exactamente las mismas 36 filas troceado en piezas de 1, 7 o 14 días (comprobado en vivo).

**4. Verificación "belt-and-suspenders" de los límites.** Para descartar tanto un solapamiento (traer un día de más) como un hueco (perder un día), se comprobó en vivo, en las 5 entidades, que dos días consecutivos `X` y `X+1` -- pedidos por la ruta real de producción con el puente correcto según familia -- cumplen: `fetch(X)` y `fetch(X+1)` son disjuntos (solape 0), y su unión es exactamente `fetch([X, X+1])`. La cuenta cuadra al registro: en `concesiones`, 115.862 + 68.457 = 184.319 filas, sin solape. Un `+1` de más (en la familia exclusiva) o un límite mal puesto (en la inclusiva) habría duplicado el día frontera; un día perdido habría roto la unión. El invariante queda fijado como test permanente (`test_generic.py`).

**Detección de bajas por ventana**: `concesiones_busqueda`, `ayudasestado_busqueda`, `minimis_busqueda` y `convocatorias` también detectan bajas reales, comparando, dentro de la misma ejecución, lo traído contra las filas de la tabla cuya propia fecha de registro (`fechaAlta`/`fechaRegistro`/`fechaRecepcion`, según la entidad) cae en ese mismo rango. No se compara contra la ejecución anterior, porque eso daría falsos positivos constantes: toda fila envejece fuera de una ventana móvil tarde o temprano, sin que eso signifique baja. `partidospoliticos_busqueda` queda fuera: confirmado en vivo que su payload no expone ningún campo de fecha de registro, pese a que el documento oficial afirma que "funciona igual, con los mismos filtros y resultados" que `concesiones_busqueda`. No es así. Es una limitación real y permanente, salvo que la API cambie.

## Buenas prácticas (oficiales)

Según el documento oficial ["Buenas prácticas API SNPSAP"](https://www.infosubvenciones.es/bdnstrans/estaticos/ayuda/Buenas%20pr%C3%A1cticas%20API%20SNPSAP.pdf):

- **Límite de 10 peticiones/segundo por IP**, aplicado por `bdns-fetch`.
- **Paginación al máximo** (10.000 registros/llamada).
- **Cadencia diaria/semanal/mensual/anual por fecha de registro**: recomendación oficial, no diseño propio.
- **`terceros` no se usa**: el propio documento lo señala como redundante.
- **Reconciliación completa para detectar bajas**: las ayudas se retiran de la BDNS a los 4 años naturales siguientes a la concesión; los catálogos completos lo detectan por comparación contra todo el estado actual. Para los grandes endpoints incrementales, donde reconciliar contra 20M+ filas cada vez es inviable, se usa en su lugar una comparación acotada a la fecha de registro propia de cada fila (ver más arriba).

## Limitaciones conocidas

- `organos_codigo` / `organos_codigoadmin` sin implementar (grupo H).
- `partidospoliticos_busqueda` sin detección de bajas: a diferencia de las otras 4 entidades incrementales, su payload no expone ningún campo de fecha de registro.
- Registros individuales malformados se descartan con log (nivel WARNING), se cuentan en `_sync_runs.rows_skipped` y quedan registrados en `_sync_errors` (contexto + contenido truncado a 200 caracteres, enlazado por `run_id`). Nunca se guardan en las tablas sincronizadas: no tienen clave natural real, así que no se pueden versionar como una fila normal.
- Sin backfill histórico para los 5 endpoints incrementales (`concesiones_busqueda`, `ayudasestado_busqueda`, `minimis_busqueda`, `partidospoliticos_busqueda`, `convocatorias`): la ventana más ancha es `annual` (365 días), pero las ayudas siguen visibles 4 años. Si se implementa, debe reusar el mismo troceo de 7 días que ya usan `monthly`/`annual` (ver arriba) -- un rango de varios años pedido de una sola vez sí falla de forma intermitente: probado en vivo (4 años, `concesiones_busqueda`, 27,4M filas), la API devuelve `ERR_MANTENIMIENTO_BBDD` en cualquier profundidad de página, mientras que una ventana semanal en el mismo rango de fechas no falló ninguna vez en 6 intentos.

## Desarrollo

```bash
poetry install
poetry run bdns-sync --help
make test
```

## Aviso legal

Proyecto no oficial, sin afiliación con la Base de Datos Nacional de Subvenciones (BDNS) ni con el Ministerio de Hacienda. Se distribuye bajo licencia GPL v3, que excluye expresamente cualquier garantía: se usa bajo tu propia responsabilidad, sin garantía de ningún tipo y sin que el autor asuma responsabilidad alguna por daños, pérdidas de datos o usos indebidos.

Los datos sincronizados proceden del [Sistema Nacional de Publicidad de Subvenciones y Ayudas Públicas](https://www.infosubvenciones.es) y están sujetos a su propio [aviso legal](https://www.infosubvenciones.es/bdnstrans/GE/es/avisolegal) y a las [buenas prácticas de la API](https://www.infosubvenciones.es/bdnstrans/estaticos/ayuda/Buenas%20pr%C3%A1cticas%20API%20SNPSAP.pdf).

## Licencia y enlaces

- [GNU GPL v3.0](./LICENSE)
- [API oficial](https://www.infosubvenciones.es/bdnstrans/api) · [Portal BDNS](https://www.infosubvenciones.es) · [Aviso legal BDNS](https://www.infosubvenciones.es/bdnstrans/GE/es/avisolegal)
- [bdns-fetch](https://github.com/cruzlorite/bdns-fetch)
