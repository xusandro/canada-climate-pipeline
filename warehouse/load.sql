
-- Truncate the tables before loading new data Because Redshift appends by default and we want to avoid duplicate


TRUNCATE raw_stations;
TRUNCATE raw_observations;


COPY raw_observations(
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


COPY raw_stations(
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


SELECT COUNT(*) FROM raw_observations;
SELECT COUNT(*) FROM raw_stations;