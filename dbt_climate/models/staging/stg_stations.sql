select
    id as station_id,
    latitude,
    longitude,
    elevation as elevation_m,
    state as province,
    name as station_name,
    gsn_flag,
    hcn_crn_flag,
    wmo_id
from {{ source('raw', 'raw_stations') }}
