{{ config(
    materialized='incremental',
    schema='gold',
    unique_key=['station_id', 'date'],
    incremental_strategy='merge'     
) }}

with silver_data as (
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
    where year(date) = {{ var('year') }}
)

{% if is_incremental() %}

-- Solo insertamos o actualizamos los registros nuevos o modificados
select * from silver_data

{% else %}

-- Primera carga completa (full-refresh)
select * from silver_data

{% endif %}