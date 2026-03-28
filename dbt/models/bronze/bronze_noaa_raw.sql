{{ config(materialized='table', schema='bronze') }}

select 
    station_id,
    date,
    element,
    data_value,
    m_flag,
    q_flag,
    s_flag,
    obs_time
from read_parquet(
    '../bronze/noaa_ghcn/{{ var("year") }}/noaa_ghcn/noaa_daily_weather/*.parquet'
)
