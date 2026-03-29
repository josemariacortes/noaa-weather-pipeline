{{ config(materialized='table', schema='gold') }}

with source as (
    select line
    from read_csv(
        '{{ var("reference_path") }}/ghcnd_stations.txt',
        delim = '\n',
        columns = {'line': 'string'},
        header = false
    )
)
-- ID            1-11   Character
-- LATITUDE     13-20   Real
-- LONGITUDE    22-30   Real
-- ELEVATION    32-37   Real
-- STATE        39-40   Character
-- NAME         42-71   Character
-- GSN FLAG     73-75   Character
-- HCN/CRN FLAG 77-79   Character
-- WMO ID       81-85   Character
select
    trim(substr(line, 1, 11)) as station_id,
    substr(line, 13, 8)::double as latitude,
    substr(line, 22, 9)::double as longitude,
    substr(line, 32, 6)::double as elevation,
    trim(substr(line, 39, 2)) as state,
    trim(substr(line, 42, 30)) as station_name,
    trim(substr(line, 73, 3)) as gsn_flag,
    trim(substr(line, 77, 3)) as hcn_crn_flag,
    trim(substr(line, 81)) as wmo_id
from source



