import os
import subprocess
import sys
from datetime import date

# Configuración del proyecto
HOY = date.today()
ANIO = int(sys.argv[1]) if len(sys.argv) > 1 else HOY.year
TEST_MODE = '--test' in sys.argv

print(f'🚀 Iniciando orquestación completa del pipeline NOAA Weather (año {ANIO})')
print(f'   Modo: {"TEST" if TEST_MODE else "PRODUCCIÓN"}')
print('=' * 80)

# 1. Ingesta Bronze con dlt
print('\n📥 1. Ejecutando ingesta Bronze (dlt)...')
os.chdir('ingest')
cmd_dlt = [
    sys.executable, 'main.py',
    str(ANIO),
    '--test' if TEST_MODE else ''
]
result = subprocess.run([c for c in cmd_dlt if c], check=True)
os.chdir('..')

# 2. Transformaciones Silver + Gold + Maestros de referencia (dbt)
print('\n🔄 2. Ejecutando transformaciones con dbt (Silver → Gold + Reference Data)...')
os.chdir('dbt')
cmd_dbt = [
    'dbt', 'build',
    '--select', 'gold_ref_countries gold_ref_stations gold_ref_inventory gold_ref_states gold_dim_country gold_dim_station_enriched silver_noaa_clean gold_fact_observations',
    '--full-refresh' if TEST_MODE else '',
    '--vars', f'"{{"year": {ANIO}}}"'
]
result = subprocess.run([c for c in cmd_dbt if c], check=True)
os.chdir('..')

print('\n✅ Pipeline completado con éxito!')
print(f'   Año procesado: {ANIO}')
print(f'   Capas cargadas: Bronze → Silver → Gold (incluyendo maestros de referencia)')
print(f'   Tablas finales disponibles en DuckDB: gold_fact_observations, gold_dim_station_enriched, gold_dim_country, ...')