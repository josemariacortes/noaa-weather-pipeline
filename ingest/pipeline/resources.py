import dlt
from noa_loader.loader import load_noaa_csv_file, load_noaa_csv_url


@dlt.resource(
    name='noaa_daily_weather',
    write_disposition='replace'
)
def noaa_weather_resource(year: int, test_mode: bool = False, local_file: bool = None):
    '''Resource dlt que usa tus funciones de loader.'''
    if local_file:
        # Modo local (usamos tu función load_noaa_csv_file)
        df = load_noaa_csv_file(local_file)
        yield df
    else:
        # Modo URL (usamos tu función load_noaa_csv_url)
        for chunk in load_noaa_csv_url(year, test_mode):
            yield chunk