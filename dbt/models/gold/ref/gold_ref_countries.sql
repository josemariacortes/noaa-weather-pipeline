{{ config(materialized='table', schema='gold') }}

select 
    column0 as country_code,
    trim(column1) as country_name
from read_csv(
    '{{ var("reference_path") }}/ghcnd_countries.txt',
    delim = ' +',
    header = false,
    columns = {
        'column0': 'string', 
        'column1': 'string'
    }
)