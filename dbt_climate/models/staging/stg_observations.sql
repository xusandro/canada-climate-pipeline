-- stg_observations.sql
select
    id as station_id,
    date as observation_date,
    tmax as tmax_c,
    tmin as tmin_c,
    tavg as tavg_c,
    prcp as prcp_mm,
    snow as snow_fall_mm,
    snwd as snwd_depth_mm
from {{ source('raw', 'raw_observations') }}