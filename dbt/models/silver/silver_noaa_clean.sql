{{ config(
    materialized='table',
    schema='silver'
) }}

with raw as (
    select * from {{ ref('bronze_noaa_raw') }}
),

pivoted as (
    select
        station_id,
        date,
        max(case when element = 'TMAX' then data_value / 10.0 end) as tmax_c,   -- temperatura máxima en ºC
        max(case when element = 'TMIN' then data_value / 10.0 end) as tmin_c,   -- temperatura mínima en ºC
        max(case when element = 'PRCP' then data_value / 10.0 end) as prcp_mm,  -- precipitación en mm
        max(case when element = 'SNOW' then data_value end)        as snow_mm,
        max(case when element = 'SNWD' then data_value end)        as snwd_mm,
        max(case when element = 'AWND' then data_value / 10.0 end) as awnd_ms,  -- velocidad viento media
        string_agg(distinct m_flag, '') as m_flags,
        string_agg(distinct q_flag, '') as q_flags
    from raw
    group by station_id, date
)

select
    *,
    (tmax_c + tmin_c) / 2.0 as tavg_c,                    -- temperatura media calculada
    case when prcp_mm > 0 then true else false end as rained,
    case when snow_mm > 0 then true else false end as snowed
from pivoted