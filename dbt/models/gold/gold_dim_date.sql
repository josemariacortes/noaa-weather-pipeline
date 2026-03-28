{{ config(materialized='table', schema='gold') }}

select
    date,
    year(date) as year,
    month(date) as month,
    day(date) as day,
    dayofweek(date) as weekday,
    case when month(date) in (12,1,2) then 'Winter'
         when month(date) in (3,4,5) then 'Spring'
         when month(date) in (6,7,8) then 'Summer'
         else 'Autumn' end as season
from {{ ref('silver_noaa_clean') }}
group by date