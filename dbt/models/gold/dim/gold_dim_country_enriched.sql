{{ config(
    materialized='table',
    schema='gold'
) }}

-- Un ejemplo de dimensión enriquecida que combina información de países y estaciones y agrega una categorización por continente. 
-- Para que estuvier completa tendriamos que teneer una tabla con los continentes y hacer un join con esa tabla, pero para este ejemplo lo hacemos con un case. Con unos cuantos paises.


select 
    c.country_code,
    c.country_name,
    -- Enriquecimiento número de estaciones por país. 
    case 
        when c.country_code = 'US' then 'North America'
        when c.country_code in ('CA','MX') then 'North America'
        when c.country_code in ('BR','AR','CO') then 'South America'
        when c.country_code in ('ES','FR','IT','DE','UK') then 'Europe'
        when c.country_code in ('AU','NZ') then 'Oceania'
        else 'Other'
    end as continent,
    count(distinct s.station_id) as num_stations
from {{ ref('gold_ref_countries') }} c
left join {{ ref('gold_ref_stations') }} s 
    on substr(s.station_id, 1, 2) = c.country_code
group by 
    c.country_code,
    c.country_name
order by num_stations desc