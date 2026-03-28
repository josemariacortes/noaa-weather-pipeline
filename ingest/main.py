import os
import sys
from datetime import date
from dlt.destinations import filesystem
import dlt
from dotenv import load_dotenv

# Importamos tu loader (estás dentro de la carpeta ingest/)
#from noa_loader.loader import load_noaa_csv_file
from pipeline.resources import noaa_weather_resource

# Solución definitiva al error de timezone en Windows
import tzdata
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

load_dotenv()

# En modo debug usamos el año actual
HOY = date.today()
ANIO = HOY.year

def main():
    if len(sys.argv) < 2:
        print('❌ Uso: python main.py <AÑO> [--test]')
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
        print('   Pipeline idempotente → no se procesa nada.')
        return
    
    modo = '(MODO TEST)' if test_mode else ''
    print(f'🚀 Iniciando pipeline {modo} para año {year}')
    
    try:
        # === SINTAXIS CORRECTA PARA WINDOWS ===
        abs_path = os.path.abspath(bronze_path).replace('\\', '/')
        bucket_url = f'file:///{abs_path}'
        
        destination = filesystem(bucket_url=bucket_url)
        
        pipeline = dlt.pipeline(
            pipeline_name='noaa_weather_bronze',
            destination=destination,
            dataset_name='noaa_ghcn'
        )
        
        local_file_path = os.path.abspath(f'data\\{year}.csv').replace('\\', '/')
        load_info = pipeline.run(
        #    load_noaa_csv_file(local_path),
            noaa_weather_resource(year, test_mode=test_mode),
            table_name='noaa_daily_weather'
        )
        
        print(f'✅ Ingesta completada para el año {year}')
        print(load_info)
        
    except Exception as e:
        print(f'❌ ERROR GENERAL en el pipeline: {type(e).__name__}: {e}')

if __name__ == '__main__':
    main()