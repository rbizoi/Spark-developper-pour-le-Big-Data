SELECT station, visibilite, precipitations24, tendpression24
FROM meteopartitionannee
ORDER BY station, visibilite DESC, precipitations24;

SELECT station, visibilite, precipitations24, tendpression24
FROM meteopartitionannee
ORDER BY station, visibilite DESC,
         precipitations24 NULLS LAST, tendpression24 DESC NULLS FIRST;

SELECT station, visibilite, precipitations24, tendpression24
FROM meteopartitionannee
SORT BY station, visibilite DESC,
        precipitations24 NULLS LAST, tendpression24 DESC NULLS FIRST;

SET spark.sql.shuffle.partitions = 2;
--spark.conf.set("spark.sql.shuffle.partitions",2)

SELECT ville, ROUND(altitude,-2) AS altitude
FROM meteo_villes
WHERE altitude > 100
DISTRIBUTE  BY altitude;

SELECT ville, ROUND(altitude,-2) AS altitude
FROM meteo_villes
WHERE altitude > 100
CLUSTER  BY altitude;
