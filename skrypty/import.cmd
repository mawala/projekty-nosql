set PGPASSWORD=123456

curl http://stat-computing.org/dataexpo/2009/airports.csv | pgfutter --pass "123456" --table "port" csv
psql -U "postgres" -f create-port.sql

curl http://stat-computing.org/dataexpo/2009/carriers.csv | pgfutter --pass "123456" --table "carrier" csv
psql -U "postgres" -f create-carrier.sql

curl http://stat-computing.org/dataexpo/2009/2008.csv.bz2 > temp.csv.bz2
7z e temp.csv.bz2 -so | pgfutter --pass "123456" --table "ontime" csv
del temp.csv.bz2
psql -U "postgres" -f create-ontime.sql

