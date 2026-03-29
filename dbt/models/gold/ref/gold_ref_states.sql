{{ config(materialized='table', schema='gold') }}

with source as (
    select line
    from read_csv(
        '{{ var("reference_path") }}/ghcnd_states.txt',
        delim = '\n',
        columns = {'line': 'string'},
        header = false
    )
)
-- CODE          1-2    Character
-- NAME         4-50    Character
select 
    substr(line, 1, 2) as state_code,
    trim(substr(line, 4)) as state_name
from source