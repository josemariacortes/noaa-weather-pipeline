{{ config(materialized='table', schema='gold') }}

select distinct
    station_id,
    substr(station_id, 1, 2) as country_code,
    substr(station_id, 3, 9) as station_code
from {{ ref('silver_noaa_clean') }}