show databases
use gest_comm

SELECT COUNT(*) AS C1, COUNT(NO_COMMANDE) AS C2,
      COUNT(DISTINCT NO_COMMANDE) AS C3, COUNT(DATE_PAIEMENT) AS C4
FROM factures;

SELECT ROUND(SUM(t),2) as SUM,
       ROUND(AVG(t),2) as AVG,
       ROUND(MIN(t),2) as MIN,
       ROUND(MAX(t),2) as MAX,
       ROUND(COUNT(t),2) as COUNT,
       ROUND(STDDEV(t),2) as STDDEV ,
       ROUND(STDDEV_SAMP(t),2) as STDDEV_S,
       ROUND(VARIANCE(t),2) as VARIANCE ,
       ROUND(VAR_POP(t),2) as VAR_POP,
       ROUND(COVAR_POP(t,rr3),2) as COVAR_P,
       ROUND(COVAR_SAMP(t,rr3),2) as COVAR_S
FROM coursspark3.meteoinitialep

SELECT * FROM
(SELECT station,
  annee,
  mois,
  ROUND(AVG(temperature),2) as temp,
  ROUND(SUM(COALESCE(precipitations3,
    precipitations24/8,
    precipitations12/4,
    precipitations6/2,
    precipitations1*3)),2) as prec,
    ROUND(AVG(humidite),2) as hum
    FROM meteopartitionannee
    GROUP BY station,annee,mois
    ORDER BY station,annee DESC,mois) LIMIT 5;

SELECT station,
        annee,
        mois,
        ROUND(AVG(temperature),2) as temp,
        ROUND(SUM(COALESCE(precipitations3,
                     precipitations24/8,
                     precipitations12/4,
                     precipitations6/2,
                     precipitations1*3)),2) as prec,
        ROUND(AVG(humidite),2) as hum
FROM meteopartitionannee
GROUP BY station,annee,mois
HAVING AVG(temperature) > 29
       AND AVG(humidite) > 82;

 SELECT * FROM
   (SELECT station,
         annee,
         mois,
         ROUND(AVG(temperature),2) as temp,
         ROUND(SUM(COALESCE(precipitations3,
                      precipitations24/8,
                      precipitations12/4,
                      precipitations6/2,
                      precipitations1*3)),2) as prec,
         ROUND(AVG(humidite),2) as hum
  FROM meteopartitionannee
  GROUP BY ROLLUP(station,annee,mois)
  ORDER BY station,annee DESC,mois) LIMIT 200;
  SELECT * FROM
  (SELECT station,
        annee,
        mois,
        ROUND(AVG(temperature),2) as temp,
        ROUND(SUM(COALESCE(precipitations3,
                     precipitations24/8,
                     precipitations12/4,
                     precipitations6/2,
                     precipitations1*3)),2) as prec,
        ROUND(AVG(humidite),2) as hum
FROM meteopartitionannee
WHERE annee > 2018
GROUP BY CUBE(station,annee,mois)
ORDER BY station,annee DESC,mois) LIMIT 120;

SELECT station,
        annee,
        mois,
        ROUND(AVG(temperature),2) as temp,
        ROUND(SUM(COALESCE(precipitations3,
                     precipitations24/8,
                     precipitations12/4,
                     precipitations6/2,
                     precipitations1*3)),2) as prec,
        ROUND(AVG(humidite),2) as hum
FROM meteopartitionannee
WHERE station < 7050
  and annee   > 2018
  and mois    < 5
GROUP BY GROUPING SETS(station,annee,(annee,mois))
ORDER BY station,annee,mois;

SELECT  GROUPING_ID(station,annee,mois) AS GI,
station,
annee,
mois,
ROUND(AVG(temperature),2) as temp,
ROUND(SUM(COALESCE(precipitations3,
  precipitations24/8,
  precipitations12/4,
  precipitations6/2,
  precipitations1*3)),2) as prec,
  ROUND(AVG(humidite),2) as hum
  FROM meteopartitionannee
  WHERE station < 7050
  and annee   > 2018
  and mois    < 5
  GROUP BY CUBE(station,annee,mois)
  ORDER BY station,annee DESC,mois
