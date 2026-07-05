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
- Sin backfill histórico para `convocatorias` (cada registro es una llamada real).

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
