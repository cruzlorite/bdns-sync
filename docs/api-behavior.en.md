# BDNS API behavior: date windows

[🇪🇸 Spanish version](./api-behavior.md)

This document records the actual behavior of the BDNS API with respect to date parameters, verified through live tests against the service. It is documented in detail because these behaviors are subtle, absent from the official documentation (or contradicted by it), and mishandling them causes silent data loss.

Each claim states the empirical check that supports it.

## 1. Internal convention: inclusive day range

In `bdns-sync`, a window is a day range **inclusive on both ends**: the `daily` window for day `X` means "the records registered on day `X`". The upper end of every window is *yesterday* (`date.today() - 1`), because the current day's data is not final until the following morning.

## 2. Upper-bound semantics per endpoint

The API has two families of date parameters, and their upper bounds behave in **opposite** ways:

| Family | Parameters | Endpoints | Upper bound | Live check (day `D`) |
|---|---|---|---|---|
| Registration-date search | `fechaRegInicio` / `fechaRegFin` | `concesiones_busqueda`, `ayudasestado_busqueda`, `minimis_busqueda`, `partidospoliticos_busqueda` | **Exclusive** (excludes day `D`) | `fechaRegFin=D` returns ~0 rows for day `D`; `fechaRegFin=D+1` returns the full day `D` (concesiones: 1 row vs. 58,488) |
| Convocatorias discovery | `fechaDesde` / `fechaHasta` | `convocatorias` (discovery step) | **Inclusive** (includes day `D`) | `fechaHasta=D` returns every convocatoria with `fechaRecepcion == D`; `fechaHasta=D+1` returns days `D` and `D+1` |

The conversion between the internal convention (inclusive) and the exclusive family is centralized in a single function, `generic.to_api_upper_bound(inclusive_end)`, which adds one day to the upper bound. The `convocatorias` endpoint does not use it: its `fechaHasta` is already inclusive, and adding a day would pull in convocatorias from outside the window.

For `convocatorias`, the date parameters only apply to the discovery step (`convocatorias_busqueda`): each discovered code is then fetched in full through the detail endpoint (by `numConv`), which has no date parameters and is what gets stored.

The cost of `convocatorias` is therefore linear in the number of codes, not in the window width. Measured live with a real month (May 2026, 6,186 codes): discovery takes ~1 s, and the detail step dominates entirely. Per-call detail latency varies with server load: ~0.22 s/call under favorable conditions (one month ≈ 23 minutes), but a full run of the same month took as long as 3 h 12 min (~1.9 s/call) with a single timeout retry. The run succeeded in both regimes (6,186 rows, 0 skips); only the duration changes.

**The detail step is parallelized.** Since it's one call per code with no pagination, the original sequential loop ran far below the 10 req/s limit (0.5-4.5 req/s actual, per the regimes measured above). Parallelizing with a thread pool was tested: 10 threads against the client's shared token-bucket limiter triggered `HTTP 429 Too Many Requests` within seconds -- the token bucket starts full, so a fresh pool's first batch fires simultaneously, and the server rejects the burst even though the stated average is 10/s. It was confirmed live that the server does sustain ~9.8 req/s with zero 429s when request *starts* are spaced 100ms apart (round-robined across 5-8 threads, each blocked by that spacing before firing). The final implementation paces starts at 105ms (a small margin under the cap) across 8 threads; the same May 2026 month took 10 min 54 s in parallel versus 23 min-3h12min sequential, with zero 429s.

Consequences of mishandling this, measured live:

- Without the conversion, the `daily` window for the four `fechaReg` endpoints would return almost nothing, and every wider window would drop its most recent day.
- With chunking (see section 3), the error compounds: one day is lost per chunk boundary. A 28-day range chunked by day returned 8 rows instead of ~1.2 million.

## 3. Windows are chunked into 7-day pieces

Every window is split into pieces of at most 7 days before being queried (`generic.iter_date_chunks`). The `daily` and `weekly` windows fit in a single piece and are unaffected; `monthly` and `annual` are chunked. Two reasons, both verified against `concesiones_busqueda`:

- **Reliability.** A 4-year range (27.4 million rows) returns `ERR_MANTENIMIENTO_BBDD` intermittently at any page depth. A one-week window over the same dates did not fail once in 6 attempts.
- **Speed.** A 30-day range queried in a single call took 286.7 s; the same range chunked into weeks took 142.5 s, both without errors.

### The result does not depend on chunk size

Because the date conversion is applied per piece, the result is invariant to chunk size: a 14-day `partidospoliticos_busqueda` range returns exactly the same 36 rows whether chunked into 1-, 7-, or 14-day pieces (verified live).

The 7-day size is not speed-critical either: over a fixed 14-day `concesiones_busqueda` range (~530,000 rows), chunk sizes of 1, 3, 7, and 14 days took 51, 41, 47, and 57 s respectively — differences within the service's load noise, with no errors at any size. The 7-day size is kept as a balance: fast, reliable, and aligned with the weekly window.

## 4. Window-boundary verification

To rule out both an overlap (fetching one day too many) and a gap (dropping a day), it was verified live, across all 5 incremental entities, that two consecutive days `X` and `X+1`, queried through the real production path with the correct per-family conversion, satisfy two properties:

1. `fetch(X)` and `fetch(X+1)` are **disjoint** (zero overlap).
2. Their union is exactly `fetch([X, X+1])` (**additivity**).

The counts reconcile row by row: for `concesiones`, 115,862 + 68,457 = 184,319 rows, with no overlap. An extra `+1` on the exclusive family, or a misapplied bound on the inclusive one, would have double-counted the boundary day; a dropped day would have broken the union. The invariant is fixed as a permanent test in `tests/test_generic.py`.

## 5. Window-scoped deletion detection

The entities `concesiones_busqueda`, `ayudasestado_busqueda`, `minimis_busqueda`, `convocatorias_busqueda`, and `convocatorias` detect real deletions by comparing, within the same run, what was fetched from the API against the table rows whose own registration date (`fechaAlta`, `fechaRegistro`, or `fechaRecepcion`, depending on the entity) falls in the same range.

The comparison is never made against the previous run: that would produce constant false positives, because every row eventually ages out of a rolling window without that meaning a deletion.

`partidospoliticos_busqueda` is excluded from deletion detection: it was confirmed live, with more than 70 real rows across two different date ranges, that its payload does not expose any registration-date field. The official documentation states that this endpoint "works the same, with the same filters and results" as `concesiones_busqueda`; in practice it does not. This is a permanent limitation unless the API changes.

## 6. Historical depth per endpoint

The reach of a full historical load is determined by the API's data retention per endpoint, measured live:

| Entity | Data available back to ~ | Bounded by |
|---|---|---|
| `concesiones_busqueda` | ~4 years | 4 calendar-year retention |
| `partidospoliticos_busqueda` | ~4 years | (tracks concesiones) |
| `ayudasestado_busqueda` | ~9-10 years | 10-year retention |
| `minimis_busqueda` | ~10 years | 10-year retention |
| `convocatorias_busqueda` | ~12 years | (tracks `convocatorias`, same discovery source) |
| `convocatorias` | ~12 years | Portal start (~2014) |

These dates are not encoded in `bdns-sync`: the tool is a pure primitive and does not know each endpoint's historical depth. Just as `scripts/delta_load.sh` owns the cadence, `scripts/full_load.sh` owns the start dates and passes them via `--since`. Querying dates before the retention limit simply returns empty weeks (one cheap call each), so the script's dates are conservative floors, not exact first records.
