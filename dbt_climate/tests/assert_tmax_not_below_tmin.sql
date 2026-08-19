select station_id, observation_date, tmax_c, tmin_c
from {{ ref('fct_daily_weather') }}
where tmax_c < tmin_c