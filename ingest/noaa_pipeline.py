import dlt
import pandas as pd
import os
import sys
from dotenv import load_dotenv
from datetime import date
from dlt.destinations import filesystem

# Solución definitiva al error de timezone en Windows
import tzdata
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

load_dotenv()

def en_debug():
    return sys.gettrace() is not None

# En modo debug usamos el año actual
HOY = date.today()
ANIO = HOY.year

@dlt.resource(
    name='noaa_daily_weather',
    write_disposition='replace'
)
def noaa_weather_resource(year: int, test_mode: bool = False):
    url = f'https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz'
    print(f'🔄 Descargando NOAA GHCN-Daily del año {year}')
    print(f'   URL: {url}')
    
    try:
        # Columnas reales del fichero (sin cabecera)
        column_names = ['station_id', 'date', 'element', 'data_value', 'm_flag', 'q_flag', 's_flag', 'obs_time']
        
        chunksize = 100_000 if test_mode else 500_000
        rows_processed = 0
        max_rows = 500_000 if test_mode else None
        
        for chunk in pd.read_csv(
            url,
            compression='gzip',
            header=None,
            names=column_names,
            chunksize=chunksize,
            low_memory=False
        ):
            # Conversión manual de fecha (evita el error de timezone)
            chunk['date'] = pd.to_datetime(chunk['date'], format='%Y%m%d', errors='coerce')
            
            # Convertimos data_value a numérico
            chunk['data_value'] = pd.to_numeric(chunk['data_value'], errors='coerce')
            
            rows_processed += len(chunk)
            print(f'   Procesadas {rows_processed:,} filas...')
            yield chunk
            
            if max_rows and rows_processed >= max_rows:
                print(f'🧪 Modo TEST: detenido después de {max_rows} filas')
                break
                
    except Exception as e:
        print(f'❌ ERROR durante la descarga/lectura: {type(e).__name__}: {e}')
        raise

def main():
    if len(sys.argv) < 2:
        print('❌ Uso: python noaa_pipeline.py <AÑO> [--test]')
        year = ANIO
        test_mode = True
    else:
        year = int(sys.argv[1])
        test_mode = '--test' in sys.argv
    
    bronze_path = f'../bronze/noaa_ghcn/{year}'
    os.makedirs(bronze_path, exist_ok=True)
    
    parquet_file = f'{bronze_path}/noaa_daily_weather.parquet'
    
    if os.path.exists(parquet_file) and not test_mode:
        print(f'✅ Archivo bronze ya existe: {parquet_file}')
        print('   Pipeline idempotente → no se descarga nada.')
        return
    
    modo = '(MODO TEST)' if test_mode else ''
    print(f'🚀 Iniciando pipeline {modo} para año {year}')
    
    try:
        bucket_url = f'file://{os.path.abspath(bronze_path)}'
        destination = filesystem(bucket_url=bucket_url)
        
        pipeline = dlt.pipeline(
            pipeline_name='noaa_weather_bronze',
            destination=destination,
            dataset_name='noaa_ghcn'
        )
        
        load_info = pipeline.run(
            noaa_weather_resource(year, test_mode=test_mode),
            table_name='noaa_daily_weather'
        )
        
        print(f'✅ Ingesta completada para el año {year}')
        print(load_info)
        
    except Exception as e:
        print(f'❌ ERROR GENERAL en el pipeline: {type(e).__name__}: {e}')

if __name__ == '__main__':
    main()
