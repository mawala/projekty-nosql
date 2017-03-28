CREATE SCHEMA IF NOT EXISTS air;

DROP TABLE IF EXISTS air.carrier;

CREATE TABLE air.carrier(
    id BIGSERIAL PRIMARY KEY,
    code VARCHAR UNIQUE,
    description VARCHAR
);

INSERT INTO air.carrier(code, description)
SELECT code, description
FROM import.carrier;

DROP TABLE import.carrier;