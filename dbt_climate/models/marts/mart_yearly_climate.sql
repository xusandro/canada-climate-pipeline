select 
date_part(year, observation_date) as observation_year,
count(distinct station_id) as station_count,
count(tmax_c) as tmax_days,
count(station_id) as station_days,
avg(tmax_c) as avg_tmax_c,
avg(tmin_c) as avg_tmin_c,
avg(tavg_c) as avg_tavg_c,
avg(prcp_mm) as avg_prcp_mm,
avg(snow_fall_mm) as avg_snow_mm,
avg(snwd_depth_mm) as avg_snwd_mm
from {{ ref('stg_observations') }}
group by date_part(year, observation_date)
