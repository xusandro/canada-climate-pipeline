

CREATE TABLE IF NOT EXISTS raw_stations(
    id CHAR(11) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    elevation DOUBLE PRECISION,
    state VARCHAR(2),
    name VARCHAR(64),
    gsn_flag VARCHAR(3),
    hcn_crn_flag VARCHAR(3),
    wmo_id VARCHAR(5)
)
DISTSTYLE ALL
ENCODE AUTO;


CREATE TABLE IF NOT EXISTS raw_observations(
    id CHAR(11) NOT NULL,
    date DATE NOT NULL,
    tmax DOUBLE PRECISION,
    tmin DOUBLE PRECISION,
    prcp DOUBLE PRECISION,
    tavg DOUBLE PRECISION,
    snow INTEGER,
    snwd INTEGER
)
DISTKEY(id)
COMPOUND SORTKEY(date)
ENCODE AUTO;