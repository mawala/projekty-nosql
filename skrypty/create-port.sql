CREATE SCHEMA IF NOT EXISTS air;

DROP TABLE IF EXISTS air.port;

CREATE TABLE air.port(
    id BIGSERIAL PRIMARY KEY,
    iata VARCHAR UNIQUE,
    airport VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    lat FLOAT,
    long FLOAT
);

INSERT INTO air.port(iata, airport, city, state, country, lat, long)
SELECT iata, airport, city, state, country, lat::float, long::float
FROM import.port;

DROP TABLE import.port;