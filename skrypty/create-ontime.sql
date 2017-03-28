CREATE SCHEMA IF NOT EXISTS air;

DROP TABLE IF EXISTS air.ontime;

CREATE TABLE IF NOT EXISTS air.ontime (
  id BIGSERIAL PRIMARY KEY,
  Year int,
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime int DEFAULT NULL,
  CRSDepTime int DEFAULT NULL,
  ArrTime int DEFAULT NULL,
  CRSArrTime int DEFAULT NULL,
  UniqueCarrier varchar REFERENCES air.carrier(code),
  FlightNum int DEFAULT NULL,
  TailNum varchar(8),
  ActualElapsedTime int DEFAULT NULL,
  CRSElapsedTime int DEFAULT NULL,
  ArrDelay int DEFAULT NULL,
  DepDelay int DEFAULT NULL,
  Origin varchar REFERENCES air.port(iata),
  Dest varchar REFERENCES air.port(iata),
  Distance int DEFAULT NULL,
  Cancelled int DEFAULT NULL,
  CancellationCode varchar(1)
);

INSERT INTO air.ontime(Year, Month, DayOfMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, ArrDelay, DepDelay, Origin, Dest, Distance, Cancelled, CancellationCode)
SELECT Year::int,
 Month::int,
 DayOfMonth::int,
 DayOfWeek::int,
 NULLIF(DepTime,'NA')::int,
 NULLIF(CRSDepTime,'NA')::int,
 NULLIF(ArrTime, 'NA')::int,
 NULLIF(CRSArrTime, 'NA')::int,
 UniqueCarrier,
 NULLIF(FlightNum,'NA')::int,
 TailNum,
 NULLIF(ActualElapsedTime,'NA')::int,
 NULLIF(CRSElapsedTime,'NA')::int,
 NULLIF(ArrDelay,'NA')::int,
 NULLIF(DepDelay,'NA')::int,
 Origin, 
 Dest, 
 NULLIF(Distance,'NA')::int,
 NULLIF(Cancelled,'NA')::int,
 CancellationCode
FROM import.ontime;

DROP TABLE import.ontime;