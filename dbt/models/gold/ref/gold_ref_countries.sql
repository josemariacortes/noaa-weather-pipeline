{{ config(materialized='table', schema='gold') }}

with raw as (
    select line
    from read_csv(
        '{{ var("reference_path") }}/ghcnd_countries.txt',
        delim = '\n',
        columns = {'line': 'string'},
        header = false
    )
)
-- CODE          1-2    Character
-- NAME          4-64   Character
select
    trim(substr(line, 1, 2)) as country_code,
    trim(substr(line, 4)) as country_name
from raw
