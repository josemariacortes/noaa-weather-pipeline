# Introduccion
Como origen de los datos hemos seleccionado la *National Centers for Environmental Information*

El proyecto lo hemos basado en los dato meteorologicos proporcionados por la pagina del gobierno de los EEUU
https://www.ncei.noaa.gov/
Vamos a recoger los datos diarios de todas las estaciones meteorologicas del mundo, que vienen agrupadas por años:
La del año en curso se actualiza perodicamente con los datos actuales.
https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/Anual
https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/ghcnd_YYYY.tar.gz
El formato de los datos está descrio los documento noaa_readme-by_year.txt
La estructura de la web en noaa_readme.txt

La aplicación descarga los datos del año indicada y los añade en la BBDD si no están.
Si se ejecuta con distintos años los va acumulando.

Hace distrintas transformaciones y validaciones de los datos para que estén dentro de los parámetros esperados.

Finalmente se ha crado un dasboar donde se representamn distintas transformaciones y representacion en el mápa de las estaciones de medición.

# Como ejecutar el modelo

## 1. Pipeline completo (recomendado)
# Procesar año 2026
>python run_pipeline.py 2026          
-  Procesar año 1972
>python run_pipeline.py 1972          

## 2. Dashboard Streamlit
streamlit run streamlit_app.py

<img width="1857" height="854" alt="image" src="https://github.com/user-attachments/assets/f3acfe6a-e743-40e1-8f74-3be33c4ca1e6" />



## 3. Solo dbt (transformaciones)
cd dbt
dbt build --full-refresh --vars '{"year": 2026}'

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

** Orquestación **

# Modo normal (año actual)
python run_pipeline.py

# Modo test (año 1972 con datos limitados)
python run_pipeline.py 1972 --test

