CREATE TABLE IF NOT EXISTS meteoInitialeO
STORED AS ORC
AS
SELECT * FROM parquet.`/user/spark/donnees/meteo_parquet`;

CREATE TABLE IF NOT EXISTS meteoInitialeP
STORED AS PARQUET
AS
SELECT * FROM parquet.`/user/spark/donnees/meteo_parquet`;

CREATE TABLE IF NOT EXISTS meteoInitiale
AS
SELECT * FROM parquet.`/user/spark/donnees/meteo_parquet`;
