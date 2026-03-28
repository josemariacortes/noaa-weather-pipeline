import pandas as pd
from typing import Generator


def load_noaa_csv_file(path: str) -> pd.DataFrame:
    """
    Carga un archivo NOAA GHCN-Daily en formato CSV con la estructura:
    ID, DATE(YYYYMMDD), ELEMENT, DATA VALUE, M-FLAG, Q-FLAG, S-FLAG, OBS-TIME
    """

    column_names = [
        "ID",          # 11 chars
        "DATE",        # YYYYMMDD
        "ELEMENT",     # 4 chars
        "DATA_VALUE",  # 5 chars
        "M_FLAG",      # 1 char
        "Q_FLAG",      # 1 char
        "S_FLAG",      # 1 char
        "OBS_TIME"     # 4 chars
    ]

    df = pd.read_csv(
        path,
        header=None,
        names=column_names,
        dtype=str
    )
    
    # guardamos el nunmero de filas procesadas para mostrarlo después de la limpieza
    rows_processed = len(df)
    
    print(f'   Procesadas {rows_processed:,} filas...')

    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d", errors="coerce")
    df["DATA_VALUE"] = pd.to_numeric(df["DATA_VALUE"], errors="coerce")

    return df

def load_noaa_csv_url(year: int, test_mode: bool = False) -> Generator[pd.DataFrame, None, None]:
    '''Generador para dlt que lee desde URL de NOAA (para descarga remota).'''
    url = f'https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz'
    print(f'🔄 Descargando desde URL: {url}')
    
    column_names = ['station_id', 'date', 'element', 'data_value', 'm_flag', 'q_flag', 's_flag', 'obs_time']
    chunksize = 100_000 if test_mode else 500_000
    max_rows = 500_000 if test_mode else None
    rows_processed = 0
    
    for chunk in pd.read_csv(
        url,
        compression='gzip',
        header=None,
        names=column_names,
        chunksize=chunksize,
        low_memory=False
    ):
        chunk['date'] = pd.to_datetime(chunk['date'], format='%Y%m%d', errors='coerce', utc = 'false')
        chunk['data_value'] = pd.to_numeric(chunk['data_value'], errors='coerce')
        
        rows_processed += len(chunk)
        print(f'   Procesadas {rows_processed:,} filas...')
        yield chunk
        
        if max_rows and rows_processed >= max_rows:
            print(f'🧪 Modo TEST: detenido después de {max_rows} filas')
            break