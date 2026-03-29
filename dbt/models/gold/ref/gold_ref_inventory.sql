{{ config(
    materialized='table',
    schema='gold'
) }}

select 
    column0 as station_id,
    column1 as latitude,
    column2 as longitude,
    column3 as element,
    column4 as first_year,
    column5 as last_year
from read_csv(
    '{{ var("reference_path") }}/ghcnd_inventory.txt',
    delim = ' +',
    header = false,
    columns = {
        'column0': 'string',
        'column1': 'double',
        'column2': 'double',
        'column3': 'string',
        'column4': 'integer',
        'column5': 'integer'
    }
)