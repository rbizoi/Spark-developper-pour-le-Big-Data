show databases;
use coursspark3;

DESCRIBE TABLE EXTENDED meteoInitiale;

ALTER TABLE meteoInitiale SET FILEFORMAT PARQUET;

ALTER TABLE meteoInitiale SET LOCATION
  "hdfs://jupiter.olimp.fr:8020/user/spark/donnees/meteoinitiale";

SHOW PARTITIONS meteopartitionannee;


ALTER TABLE table_identifier ADD [IF NOT EXISTS]
    ( partition_spec [ partition_spec ... ] )

ALTER TABLE table_identifier DROP [ IF EXISTS ] partition_spec [PURGE]

ALTER TABLE meteopartitionannee DROP
         IF EXISTS  PARTITION (annee=1996);
ALTER TABLE meteopartitionannee DROP
         IF EXISTS  PARTITION (annee=1997);

ALTER TABLE meteopartitionannee ADD
         IF NOT EXISTS PARTITION (annee=2021);

SELECT count(*) FROM meteopartitionannee
WHERE annee <= 1998;

TRUNCATE TABLE meteopartitionannee PARTITION(annee=1998);

SELECT count(*) FROM meteopartitionannee
WHERE annee = 1998;


TRUNCATE TABLE meteopartitionannee;
SELECT count(*) FROM meteopartitionannee;

INSERT INTO meteoPartitionAnnee
  SELECT mois,jour,numer_sta,ff,t,u,vv,pres,tend,tend24,
         rr1,rr3,rr6,rr12,rr24,date,annee
  FROM parquet.`/user/spark/donnees/meteo_parquet`;

SELECT count(*) FROM meteopartitionannee;


use coursspark3;
ANALYZE TABLE meteoinitialep COMPUTE STATISTICS FOR ALL COLUMNS;
DESCRIBE TABLE EXTENDED meteoinitialep;


show partitions meteopartitionannee;

ANALYZE TABLE meteopartitionannee PARTITION(annee=2020) COMPUTE STATISTICS FOR ALL COLUMNS;

DESCRIBE TABLE EXTENDED meteoinitialep;
ANALYZE TABLE meteopartitionannee COMPUTE STATISTICS FOR ALL COLUMNS;


TRUNCATE TABLE meteopartitionannee PARTITION(annee=2020);

INSERT INTO meteoPartitionAnnee
  SELECT mois,jour,numer_sta,ff,t,u,vv,pres,tend,tend24,
         rr1,rr3,rr6,rr12,rr24,date,annee
  FROM parquet.`/user/spark/donnees/meteo_parquet`
  WHERE annee = 2020;
ANALYZE TABLE meteopartitionannee PARTITION(annee=2020) COMPUTE STATISTICS FOR ALL COLUMNS;



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



CREATE TABLE meteoPartitionAnnee
        ( mois              INT      ,
          jour              INT      ,
          station           INT      ,
          vitesseVent       DOUBLE   ,
          temperature       DOUBLE   ,
          humidite          INT      ,
          visibilite        DOUBLE   ,
          pression          DOUBLE   ,
          tendpression3     INT      ,
          tendpression24    INT      ,
          precipitations1   DOUBLE   ,
          precipitations3   DOUBLE   ,
          precipitations6   DOUBLE   ,
          precipitations12  DOUBLE   ,
          precipitations24  DOUBLE   ,
          date              STRING   )
PARTITIONED BY (annee INT)
STORED AS PARQUET;

INSERT INTO meteoPartitionAnnee
  SELECT mois,jour,numer_sta,ff,t,u,vv,pres,tend,tend24,
         rr1,rr3,rr6,rr12,rr24,date,annee
  FROM parquet.`/user/spark/donnees/meteo_parquet`;

SELECT count(*) from meteoPartitionAnnee;


CREATE TABLE meteoClusterAnnee
        ( annee             INT,
          mois              INT      ,
          jour              INT      ,
          station           INT      ,
          vitesseVent       DOUBLE   ,
          temperature       DOUBLE   ,
          humidite          INT      ,
          visibilite        DOUBLE   ,
          pression          DOUBLE   ,
          tendpression3     INT      ,
          tendpression24    INT      ,
          precipitations1   DOUBLE   ,
          precipitations3   DOUBLE   ,
          precipitations6   DOUBLE   ,
          precipitations12  DOUBLE   ,
          precipitations24  DOUBLE   ,
          date              STRING   )
CLUSTERED BY (annee, mois, jour)
INTO 5 BUCKETS
STORED AS PARQUET;

INSERT INTO meteoClusterAnnee
  SELECT annee,mois,jour,numer_sta,ff,t,u,vv,pres,tend,tend24,
         rr1,rr3,rr6,rr12,rr24,date
FROM parquet.`/user/spark/donnees/meteo_parquet`;

SELECT count(*) from meteoClusterAnnee;

CREATE TABLE meteo_villes
USING PARQUET
LOCATION "/user/spark/donnees/meteo_villes_parquet";
SELECT count(*) FROM meteo_villes;
SELECT count(*) FROM
parquet.`/user/spark/donnees/meteo_villes_parquet`;

CREATE TABLE employes
USING PARQUET
LOCATION "/user/spark/donnees/parquet/EMPLOYES_parquet";

ADD JAR "/usr/share/spark/jars/delta-core_2.12-0.8.0-SNAPSHOT.jar";
ADD JAR "hdfs://jupiter.olimp.fr:8020/spark-jars/delta-core_2.12-0.8.0-SNAPSHOT.jar";

CREATE TABLE fournisseurs
USING DELTA
LOCATION "/user/spark/donnees/delta/FOURNISSEURS_delta";

SELECT count(*) FROM employes;
SELECT count(*) FROM parquet.`/user/spark/donnees/parquet/EMPLOYES_parquet`;
SELECT count(*) FROM fournisseurs;
SELECT count(*) FROM delta.`/user/spark/donnees/delta/FOURNISSEURS_delta`;

CREATE DATABASE IF NOT EXISTs gest_comm
LOCATION "hdfs:///user/spark/databases/gest_comm_db";
use gest_comm;

CREATE TABLE ACHETEURS                  USING PARQUET LOCATION"donnees/parquet/ACHETEURS_parquet";
CREATE TABLE ADRESSES                   USING PARQUET LOCATION"donnees/parquet/ADRESSES_parquet";
CREATE TABLE AGENCES                    USING PARQUET LOCATION"donnees/parquet/AGENCES_parquet";
CREATE TABLE CATEGORIES                 USING PARQUET LOCATION"donnees/parquet/CATEGORIES_parquet";
CREATE TABLE CLIENTS                    USING PARQUET LOCATION"donnees/parquet/CLIENTS_parquet";
CREATE TABLE COMMANDES                  USING PARQUET LOCATION"donnees/parquet/COMMANDES_parquet";
CREATE TABLE COMMISSIONNEMENTS_AGENCES  USING PARQUET LOCATION"donnees/parquet/COMMISSIONNEMENTS_AGENCES_parquet";
CREATE TABLE COMMISSIONNEMENTS_VENDEURS USING PARQUET LOCATION"donnees/parquet/COMMISSIONNEMENTS_VENDEURS_parquet";
CREATE TABLE COMMISSIONNEMENTS          USING PARQUET LOCATION"donnees/parquet/COMMISSIONNEMENTS_parquet";
CREATE TABLE COORDONEES                 USING PARQUET LOCATION"donnees/parquet/COORDONEES_parquet";
CREATE TABLE DETAILS_COMMANDES          USING PARQUET LOCATION"donnees/parquet/DETAILS_COMMANDES_parquet";
CREATE TABLE EMPLOYES                   USING PARQUET LOCATION"donnees/parquet/EMPLOYES_parquet";
CREATE TABLE FACTURES                   USING PARQUET LOCATION"donnees/parquet/FACTURES_parquet";
CREATE TABLE FOURNISSEURS               USING PARQUET LOCATION"donnees/parquet/FOURNISSEURS_parquet";
CREATE TABLE GESTIONS_STOCKS            USING PARQUET LOCATION"donnees/parquet/GESTIONS_STOCKS_parquet";
CREATE TABLE MAGASINS                   USING PARQUET LOCATION"donnees/parquet/MAGASINS_parquet";
CREATE TABLE MOUVEMENTS                 USING PARQUET LOCATION"donnees/parquet/MOUVEMENTS_parquet";
CREATE TABLE PRODUITS                   USING PARQUET LOCATION"donnees/parquet/PRODUITS_parquet";
CREATE TABLE RELANCES                   USING PARQUET LOCATION"donnees/parquet/RELANCES_parquet";
CREATE TABLE STOCKS_ENTREPOTS           USING PARQUET LOCATION"donnees/parquet/STOCKS_ENTREPOTS_parquet";
CREATE TABLE TVA_PRODUIT                USING PARQUET LOCATION"donnees/parquet/TVA_PRODUIT_parquet";
CREATE TABLE VENDEURS                   USING PARQUET LOCATION"donnees/parquet/VENDEURS_parquet";
CREATE TABLE VILLES                     USING PARQUET LOCATION"donnees/parquet/VILLES_parquet";


DROP  DATABASE gest_comm CASCADE;

CREATE DATABASE IF NOT EXISTs gest_comm
LOCATION "hdfs:///user/spark/databases/gest_comm_db";
USE gest_comm;

CREATE TABLE ACHETEURS          USING PARQUET LOCATION"donnees/parquet/ACHETEURS_parquet";
CREATE TABLE CLIENTS            USING PARQUET LOCATION"donnees/parquet/CLIENTS_parquet";
CREATE TABLE COMMANDES          USING PARQUET LOCATION"donnees/parquet/COMMANDES_parquet";
CREATE TABLE DETAILS_COMMANDES  USING PARQUET LOCATION"donnees/parquet/DETAILS_COMMANDES_parquet";
CREATE TABLE MAGASINS           USING PARQUET LOCATION"donnees/parquet/MAGASINS_parquet";

CREATE OR REPLACE VIEW clientsQuantites
AS
SELECT CL.SOCIETE AS CLIENT
       , EXTRACT ( YEAR FROM DATE_COMMANDE) AS ANNEE
       , EXTRACT ( MONTH FROM DATE_COMMANDE) AS MOIS
       , COUNT(DISTINCT CO.NO_COMMANDE) AS NB_COMMANDES
       , SUM(DC.PORT) AS PORT
       , SUM(DC.QUANTITE) AS QUANTITE
FROM CLIENTS CL
   JOIN MAGASINS MA ON MA.CODE_CLIENT = CL.CODE_CLIENT
   JOIN ACHETEURS AC ON AC.NO_MAGASIN = MA.NO_MAGASIN
   JOIN COMMANDES CO ON CO.NO_ACHETEUR = AC.NO_ACHETEUR
   JOIN DETAILS_COMMANDES DC ON DC.NO_COMMANDE = CO.NO_COMMANDE
GROUP BY CL.SOCIETE
       , EXTRACT ( YEAR FROM DATE_COMMANDE)
       , EXTRACT ( MONTH FROM DATE_COMMANDE)
ORDER BY CL.SOCIETE
       , EXTRACT ( YEAR FROM DATE_COMMANDE)
       , EXTRACT ( MONTH FROM DATE_COMMANDE);


SELECT MOIS,NB_COMMANDES,PORT,QUANTITE
FROM clientsQuantites
WHERE ANNEE = 2019 AND CLIENT = "La corne d'abondance"
ORDER BY MOIS;
