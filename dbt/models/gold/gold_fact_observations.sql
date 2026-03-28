{{ config(
    materialized='incremental',
    unique_key=['station_id', 'date'],
    schema='gold'
) }}

select
    station_id,
    date,
    tmax_c,
    tmin_c,
    tavg_c,
    prcp_mm,
    snow_mm,
    snwd_mm,
    awnd_ms,
    rained,
    snowed,
    m_flags,
    q_flags
from {{ ref('silver_noaa_clean') }}
{% if is_incremental() %}
where date >= (select max(date) from {{ this }})
{% endif %}