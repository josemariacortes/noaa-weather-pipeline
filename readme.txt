Para crear los maestros de algunos datos proporcionadosen ficheros de texto tabulados, los descargamos al proyecto como parte de el y locargamos en tablas como procesos seeds en las BBDD GOLD para su explotación. 

El formato de los datos está descrio los documento noaa_readme-by_year.txt
La estructura de la web en noaa_readme.txt

dbt seed --full-refresh


cd D:\josem\vscProjects\noaa-weather-pipeline\dbt
dbt build --select gold_ref_countries gold_ref_stations gold_ref_inventory gold_ref_states --full-refresh


cd D:\josem\vscProjects\noaa-weather-pipeline\dbt
dbt seed --select ghcnd_countries ghcnd_stations ghcnd_inventory ghcnd_states --full-refresh

cd D:\josem\vscProjects\noaa-weather-pipeline\dbt
dbt build --select gold_ref_countries gold_ref_stations gold_ref_inventory gold_ref_states --full-refresh

cd D:\josem\vscProjects\noaa-weather-pipeline\dbt
dbt build --select gold_dim_country_enriched gold_dim_station_enriched --full-refresh

** Orquestación 

# Modo normal (año actual)
python run_pipeline.py

# Modo test (año 1972 con datos limitados)
python run_pipeline.py 1972 --test