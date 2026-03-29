{{ config(materialized='table', schema='gold') }}

with source as (
    select line
    from read_csv(
        '{{ var("reference_path") }}/ghcnd_inventory.txt',
        delim = '\n',
        columns = {'line': 'string'},
        header = false
    )
)
-- ID            1-11   Character
-- LATITUDE     13-20   Real
-- LONGITUDE    22-30   Real
-- ELEMENT      32-35   Character
-- FIRSTYEAR    37-40   Integer
-- LASTYEAR     42-45   Integer
select 
    substr(line, 1, 11) as station_id,
    substr(line, 13, 8)::double as latitude,
    substr(line, 22, 9)::double as longitude,
    substr(line, 32, 4) as element,
    cast(substr(line, 37, 4) as integer)::integer as first_year,
    cast(substr(line, 42, 4) as integer)::integer as last_year
from source