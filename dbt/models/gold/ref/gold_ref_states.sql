{{ config(
    materialized='table',
    schema='gold'
) }}

select 
    column0 as state_code,
    trim(column1) as state_name
from read_csv(
    '{{ var("reference_path") }}/ghcnd_states.txt',
    delim = ' +',
    header = false,
    columns = {
        'column0': 'string',
        'column1': 'string'
    }
)