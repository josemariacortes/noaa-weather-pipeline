{{ config(
    materialized='table',
    schema='gold'
) }}

select 
    s.station_id,
    s.station_name,
    s.latitude,
    s.longitude,
    s.elevation,
    c.country_code,
    c.country_name,
    st.state_code,
    st.state_name,
    -- Coordenadas en formato geográfico (útil para mapas y análisis espacial)
    s.latitude::double as lat,
    s.longitude::double as lon,
    -- Enriquecimiento con datos de silver
    count(distinct o.date) as observation_days,
    min(o.date) as first_observation,
    max(o.date) as last_observation
from {{ ref('gold_ref_stations') }} s
left join {{ ref('gold_ref_countries') }} c 
    on substr(s.station_id, 1, 2) = c.country_code
left join {{ ref('gold_ref_states') }} st 
    on s.state = st.state_code
    and c.country_code = 'US'
left join {{ ref('silver_noaa_clean') }} o 
    on o.station_id = s.station_id
group by 
    s.station_id,
    s.station_name,
    s.latitude,
    s.longitude,
    s.elevation,
    c.country_code,
    c.country_name,
    st.state_code,
    st.state_name