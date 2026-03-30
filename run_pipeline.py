import os
import subprocess
import sys
from datetime import date

# ===================== CONFIGURACIÓN DEL PROYECTO =====================
HOY = date.today()
ANIO = int(sys.argv[1]) if len(sys.argv) > 1 else HOY.year
TEST_MODE = '--test' in sys.argv

print(f'🚀 INICIANDO ORQUESTACIÓN COMPLETA - NOAA WEATHER PIPELINE')
print(f'   Año: {ANIO}   Modo: {"TEST" if TEST_MODE else "PRODUCCIÓN"}')
print(f'   Arquitectura: Lakehouse Medallion (Bronze → Silver → Gold)')
print('=' * 90)

# ===================== 1. INGESTA BRONZE (dlt) =====================
print('\n📥 1. Ejecutando capa BRONZE (dlt - ingesta raw Parquet)...')
os.chdir('ingest')

cmd_dlt = [sys.executable, 'main.py', str(ANIO)]
if TEST_MODE:
    cmd_dlt.append('--test')

subprocess.run(cmd_dlt, check=True)
os.chdir('..')

# ===================== 2. TRANSFORMACIONES DBT (Silver + Gold) =====================
print('\n🔄 2. Ejecutando transformaciones con dbt (Silver → Gold + Reference Data)...')
os.chdir('dbt')

cmd_dbt = [
    'dbt', 'build',
    '--select', 
    'gold_ref_countries gold_ref_stations gold_ref_inventory gold_ref_states '
    'gold_dim_country gold_dim_station_enriched silver_noaa_clean gold_fact_observations',
    '--full-refresh' if TEST_MODE else '',
    '--vars', f'{{"year": {ANIO}}}'
]

# Filtramos cadenas vacías
cmd_dbt = [x for x in cmd_dbt if x]

subprocess.run(cmd_dbt, check=True)
os.chdir('..')

# ===================== FIN =====================
print('\n✅ ORQUESTACIÓN COMPLETADA CON ÉXITO')
print(f'   Año procesado: {ANIO}')
print(f'   Capas cargadas: Bronze → Silver → Gold (incluyendo maestros de referencia)')
print(f'   Modelos finales en DuckDB:')
print(f'      • gold_fact_observations')
print(f'      • gold_dim_station_enriched')
print(f'      • gold_dim_country')
print(f'      • gold_ref_countries, gold_ref_stations, ...')
print('\n🎯 Pipeline listo para producción (Databricks Lakehouse / Snowflake / BigQuery)')