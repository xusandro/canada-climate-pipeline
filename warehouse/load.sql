
-- Truncate the tables before loading new data Because Redshift appends by default and we want to avoid duplicate


TRUNCATE dim_stations;
TRUNCATE fact_observations;


COPY fact_observations(
    id,
    date,
    tmax,
    tmin,
    prcp,
    tavg,
    snow,
    snwd
) 
FROM 's3://canada-climate-pipeline/processed/observations.parquet/'
IAM_ROLE DEFAULT
FORMAT AS PARQUET;


COPY dim_stations(
    id,
    latitude,
    longitude,
    elevation,
    state,
    name,
    gsn_flag,
    hcn_crn_flag,
    wmo_id
) 
FROM 's3://canada-climate-pipeline/processed/stations.parquet/'
IAM_ROLE DEFAULT
FORMAT AS PARQUET;


SELECT COUNT(*) FROM fact_observations;
SELECT COUNT(*) FROM dim_stations;