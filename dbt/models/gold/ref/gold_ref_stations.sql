{{ config(
    materialized='table',
    schema='gold'
) }}

select 
    column0 as station_id,
    column1 as latitude,
    column2 as longitude,
    column3 as elevation,
    trim(column4) as station_name,
    trim(column5) as gsn_flag,
    trim(column6) as hcn_flag,
    trim(column7) as wmo_id
from read_csv(
    '{{ var("reference_path") }}/ghcnd_stations.txt',
    delim = ' +',   -- múltiples espacios o tabuladores
    header = false,
    columns = {
        'column0': 'string',
        'column1': 'double',
        'column2': 'double',
        'column3': 'double',
        'column4': 'string',
        'column5': 'string',
        'column6': 'string',
        'column7': 'string'
    }
)