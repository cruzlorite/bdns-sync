#!/usr/bin/env bash
#
# Orchestration for bdns-sync. This script owns all cadence/scheduling
# knowledge -- bdns-sync itself is a pure parameterized tool with no config
# file and no idea what day it is. One crontab line calls this once a day:
#
#   0 2 * * * BDNS_SYNC_TARGET_URL=bigquery://project/dataset /path/to/scripts/delta_load.sh
#
# Windows cascade, they don't replace each other: daily always runs, and
# weekly/monthly/annual are *additional* re-verification passes on the same
# day (e.g. Jan 1st on a Sunday runs daily + weekly + monthly + annual, all
# in the same invocation), per the "Endpoint types" section of the README.
set -euo pipefail

: "${BDNS_SYNC_TARGET_URL:?set BDNS_SYNC_TARGET_URL to the target DB URL}"

# groups A/B/C/F/G -- full replace every run, no window concept
bdns-sync sync sectores
bdns-sync sync actividades
bdns-sync sync finalidades
bdns-sync sync beneficiarios
bdns-sync sync instrumentos
bdns-sync sync objetivos
bdns-sync sync convocatorias_ultimas
bdns-sync sync organos
bdns-sync sync organos_agrupacion
bdns-sync sync regiones
bdns-sync sync reglamentos
bdns-sync sync sanciones_busqueda
bdns-sync sync grandesbeneficiarios_anios
bdns-sync sync grandesbeneficiarios_busqueda
bdns-sync sync planesestrategicos_busqueda
bdns-sync sync planesestrategicos
bdns-sync sync planesestrategicos_vigencia

# group D + convocatorias -- reg-date incremental, cascading windows
run_window() {
  local window="$1"
  bdns-sync sync concesiones_busqueda --window "$window"
  bdns-sync sync ayudasestado_busqueda --window "$window"
  bdns-sync sync minimis_busqueda --window "$window"
  bdns-sync sync partidospoliticos_busqueda --window "$window"
  bdns-sync sync convocatorias_busqueda --window "$window"
  bdns-sync sync convocatorias --window "$window"
}

run_window daily
if [ "$(date +%u)" = 7 ]; then run_window weekly; fi          # Sunday
if [ "$(date +%d)" = 01 ]; then run_window monthly; fi        # 1st of month
if [ "$(date +%m-%d)" = 01-01 ]; then run_window annual; fi   # Jan 1st
