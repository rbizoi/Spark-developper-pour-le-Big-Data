use gest_comm;

SELECT no_employe, fonction, salaire,
    ROUND(salaire * 100 / SUM( salaire) OVER ( ),2) AS pct_sal,
    SUM( salaire) OVER ( ) AS somme_totale
    FROM employes LIMIT 10;

SELECT no_employe, fonction, salaire,
    SUM( salaire) OVER ( PARTITION BY fonction) AS somme_salaires
    FROM employes LIMIT 18;

WITH sr_initiale (
  SELECT vl.pays                                      ,
         EXTRACT ( year  FROM date_commande) AS annee ,
         SUM(port)                           AS port
  FROM COMMANDES CO
          JOIN details_commandes dc ON dc.no_commande = co.no_commande
          JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
          JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
          JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
          JOIN villes vl            ON ad.no_ville    = vl.no_ville
  GROUP BY  vl.pays,
            EXTRACT ( year  FROM date_commande)
)
SELECT pays  ,
       annee ,
       port  ,
       SUM(port) OVER (PARTITION BY pays ) AS t_pays,
       SUM(port) OVER (PARTITION BY annee) AS t_annee
FROM sr_initiale
ORDER BY pays,annee
LIMIT 15;

WITH sr_initiale (
  SELECT vl.pays                                      ,
         EXTRACT ( year  FROM date_commande) AS annee ,
         EXTRACT ( month FROM date_commande) AS mois  ,
         SUM(port)                           AS port
  FROM COMMANDES CO
          JOIN details_commandes dc ON dc.no_commande = co.no_commande
          JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
          JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
          JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
          JOIN villes vl            ON ad.no_ville    = vl.no_ville
  WHERE EXTRACT ( year  FROM date_commande) IN (2017,2018)
    AND EXTRACT ( month FROM date_commande) <= 5
    AND vl.pays IN ('Allemagne','France')
  GROUP BY  vl.pays,
            EXTRACT ( year  FROM date_commande) ,
            EXTRACT ( month FROM date_commande)
)
SELECT pays  ,
       annee ,
       mois  ,
       port  ,
       SUM(port) OVER (PARTITION BY annee,mois ) AS t_annee_mois,
       SUM(port) OVER (PARTITION BY annee) AS t_annee,
       SUM(port) OVER (PARTITION BY mois ) AS t_mois,
       SUM(port) OVER (PARTITION BY pays ) AS t_pays
FROM sr_initiale
ORDER BY pays,annee,mois;

WITH sr_initiale (
  SELECT vl.pays                                      ,
         EXTRACT ( year  FROM date_commande) AS annee ,
         EXTRACT ( month FROM date_commande) AS mois  ,
         SUM(port)                           AS port
  FROM COMMANDES CO
          JOIN details_commandes dc ON dc.no_commande = co.no_commande
          JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
          JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
          JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
          JOIN villes vl            ON ad.no_ville    = vl.no_ville
  WHERE EXTRACT ( year  FROM date_commande) IN (2017,2018)
    AND EXTRACT ( month FROM date_commande) <= 5
    AND vl.pays IN ('Allemagne','France')
  GROUP BY  vl.pays,
            EXTRACT ( year  FROM date_commande) ,
            EXTRACT ( month FROM date_commande)
)
SELECT pays  ,
       annee ,
       mois  ,
       port  ,
       SUM(port) OVER (PARTITION BY annee,mois
                        ORDER BY pays) AS t_annee_mois,
       SUM(port) OVER (PARTITION BY annee
                        ORDER BY pays, mois) AS t_annee,
       SUM(port) OVER (PARTITION BY pays
                        ORDER BY annee, mois) AS t_pays
FROM sr_initiale
ORDER BY pays,annee,mois;

WITH sr_initiale (
  SELECT EXTRACT ( year  FROM date_commande) AS annee ,
         EXTRACT ( month FROM date_commande) AS mois  ,
         SUM(port)                           AS port
  FROM COMMANDES CO
          JOIN details_commandes dc ON dc.no_commande = co.no_commande
  WHERE EXTRACT ( year  FROM date_commande) IN (2017,2018)
    AND EXTRACT ( month FROM date_commande) <= 5
    AND dc.ref_produit = 1
  GROUP BY  EXTRACT ( year  FROM date_commande) ,
            EXTRACT ( month FROM date_commande)
)
SELECT annee ,
       mois  ,
       port  ,
       SUM(port) OVER ( PARTITION BY annee
                        ORDER BY mois) AS t_Pannee_Omois,
       SUM(port) OVER ( ORDER BY annee, mois) AS t_Oannee_mois
FROM sr_initiale
ORDER BY annee,mois

WITH sr_initiale (
  SELECT to_date(date_commande) AS date_commande,
         SUM(port)              AS port
  FROM COMMANDES CO
          JOIN details_commandes dc
            ON dc.no_commande = co.no_commande
  WHERE to_date(date_commande) between
        to_date('01/05/2019','dd/MM/yyyy')
        AND to_date('15/05/2019','dd/MM/yyyy')
    AND dc.ref_produit = 1
  GROUP BY  to_date(date_commande)
)
SELECT date_commande ,
       port,
       SUM(port) OVER ( ORDER BY date_commande) AS sc1,
       SUM(port) OVER ( ORDER BY date_commande
                        RANGE BETWEEN UNBOUNDED PRECEDING
                              AND CURRENT ROW ) AS sc1,
       SUM(port) OVER ( ORDER BY date_commande
                        RANGE 1 PRECEDING )     AS sc2,
       ROUND(
       AVG(port) OVER ( ORDER BY date_commande
                        RANGE BETWEEN 1 PRECEDING AND
                                      1 PRECEDING    ),2) AS sc3,
       ROUND(
       AVG(port) OVER ( ORDER BY date_commande
                        RANGE BETWEEN 1 FOLLOWING AND
                                      1 FOLLOWING    ),2) AS sc4,
       SUM(port) OVER ( ORDER BY date_commande
                        RANGE BETWEEN 1 PRECEDING AND
                                      1 FOLLOWING    ) AS sc5
FROM sr_initiale;

WITH sr_initiale (
  SELECT to_date(date_commande) AS date_commande,
         SUM(port)              AS port
  FROM COMMANDES CO
          JOIN details_commandes dc
            ON dc.no_commande = co.no_commande
  WHERE TO_DATE(date_commande) between
        TO_DATE('01/05/2019','dd/MM/yyyy')
        AND TO_DATE('15/05/2019','dd/MM/yyyy')
    AND dc.ref_produit = 1
  GROUP BY  to_date(date_commande)
)
SELECT  date_commande ,
        port,
        ROUND(
          AVG(port) OVER ( ORDER BY date_commande
                    RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING),2) AS AP_RANGE,
        ROUND(
          AVG(port) OVER ( ORDER BY date_commande
                    ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),2)  AS AP_ROWS
FROM sr_initiale;


WITH sr_initiale AS (
  SELECT to_date(date_commande) AS date_commande,
         SUM(port)              AS port
  FROM COMMANDES CO
          JOIN details_commandes dc
            ON dc.no_commande = co.no_commande
  WHERE TO_DATE(date_commande) between
        TO_DATE('01/05/2019','dd/MM/yyyy')
        AND TO_DATE('15/05/2019','dd/MM/yyyy')
    AND dc.ref_produit = 1
  GROUP BY  to_date(date_commande)
),
dim_temps AS
(
  SELECT TO_DATE('01/05/2019','dd/MM/yyyy') + CAST(id AS INT) AS date_commande
  FROM RANGE(14)
),
sr_finale AS
(  SELECT dt.date_commande, sr.port
   FROM dim_temps dt LEFT OUTER JOIN sr_initiale sr
        ON dt.date_commande = sr.date_commande
)
SELECT  date_commande ,
        port,
        ROUND(
          AVG(port) OVER ( ORDER BY date_commande
                    RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING),2) AS AP_RANGE,
        ROUND(
          AVG(port) OVER ( ORDER BY date_commande
                    ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),2)  AS AP_ROWS
FROM sr_finale;

WITH sr_initiale (
  SELECT vl.pays                                      ,
         EXTRACT ( year  FROM date_commande) AS annee ,
         EXTRACT ( month FROM date_commande) AS mois  ,
         SUM(port)                           AS port
  FROM COMMANDES CO
          JOIN details_commandes dc ON dc.no_commande = co.no_commande
          JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
          JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
          JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
          JOIN villes vl            ON ad.no_ville    = vl.no_ville
  WHERE EXTRACT ( year  FROM date_commande) IN (2017,2018)
    AND EXTRACT ( month FROM date_commande) <= 5
    AND vl.pays IN ('Allemagne','France')
  GROUP BY  vl.pays,
            EXTRACT ( year  FROM date_commande) ,
            EXTRACT ( month FROM date_commande)
)
SELECT pays  ,
       annee ,
       mois  ,
       port  ,
       ROW_NUMBER() OVER ( PARTITION BY annee
                           ORDER BY mois, pays      ) AS rn1,
       ROW_NUMBER() OVER ( ORDER BY annee,mois,pays ) AS rn2,
       ROW_NUMBER() OVER ( PARTITION BY annee
                           ORDER BY pays, mois      ) AS rn3,
       ROW_NUMBER() OVER ( PARTITION BY pays
                           ORDER BY annee, mois     ) AS rn4,
       ROW_NUMBER() OVER ( ORDER BY annee,mois,pays ) AS rn5,
       ROW_NUMBER() OVER ( ORDER BY pays,annee,mois ) AS rn6
FROM sr_initiale
ORDER BY pays,annee,mois;

WITH sr_initiale (
SELECT vl.pays                                      ,
       EXTRACT ( year  FROM date_commande) AS annee ,
       EXTRACT ( month FROM date_commande) AS mois  ,
       SUM(port)                           AS port
FROM COMMANDES CO
        JOIN details_commandes dc ON dc.no_commande = co.no_commande
        JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
        JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
        JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
        JOIN villes vl            ON ad.no_ville    = vl.no_ville
WHERE EXTRACT ( year  FROM date_commande) IN (2017,2018)
  AND EXTRACT ( month FROM date_commande) <= 5
  AND vl.pays IN ('Allemagne','France')
GROUP BY  vl.pays,
          EXTRACT ( year  FROM date_commande) ,
          EXTRACT ( month FROM date_commande)
)
SELECT pays  ,
     annee ,
     mois  ,
     port  ,
     RANK(port) OVER ( PARTITION BY pays
                       ORDER BY annee,mois       ) AS c1,
     DENSE_RANK(port) OVER ( PARTITION BY pays
                       ORDER BY annee,mois       ) AS c2,
     ROUND(
     PERCENT_RANK(port) OVER ( PARTITION BY pays
                    ORDER BY annee,mois       ),2) AS c3,
     CUME_DIST() OVER ( PARTITION BY pays
                       ORDER BY annee,mois       ) AS c4
FROM sr_initiale
ORDER BY pays,annee,mois;


WITH sr_initiale (
  SELECT vl.pays                                      ,
         EXTRACT ( year  FROM date_commande) AS annee ,
         EXTRACT ( month FROM date_commande) AS mois  ,
         SUM(port)                           AS port
  FROM COMMANDES CO
          JOIN details_commandes dc
               ON dc.no_commande = co.no_commande
          JOIN acheteurs ac
               ON co.no_acheteur = ac.no_acheteur
          JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
          JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
          JOIN villes vl            ON ad.no_ville    = vl.no_ville
  WHERE EXTRACT ( year  FROM date_commande) IN (2017,2018)
    AND EXTRACT ( month FROM date_commande) <= 5
    AND vl.pays IN ('Allemagne','France')
  GROUP BY  vl.pays,
            EXTRACT ( year  FROM date_commande) ,
            EXTRACT ( month FROM date_commande)
)
SELECT pays  ,
       annee ,
       mois  ,
       port  ,
       RANK(port) OVER ( PARTITION BY pays
                         ORDER BY annee,mois       ) AS c1,
       DENSE_RANK(port) OVER ( PARTITION BY pays
                         ORDER BY annee,mois       ) AS c2,
       ROUND(
       PERCENT_RANK(port) OVER ( PARTITION BY pays
                      ORDER BY annee,mois       ),2) AS c3,
       ROUND(
       CUME_DIST() OVER ( PARTITION BY pays
                         ORDER BY annee,mois    ),2) AS c4,
       NTILE(4) OVER ( PARTITION BY pays
                         ORDER BY annee,mois       ) AS c5
FROM sr_initiale
ORDER BY pays,annee,mois;

WITH sr_initiale (
  SELECT vl.pays                                      ,
         EXTRACT ( year  FROM date_commande) AS annee ,
         EXTRACT ( month FROM date_commande) AS mois  ,
         SUM(port)                           AS port
  FROM COMMANDES CO
          JOIN details_commandes dc
               ON dc.no_commande = co.no_commande
          JOIN acheteurs ac
               ON co.no_acheteur = ac.no_acheteur
          JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
          JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
          JOIN villes vl            ON ad.no_ville    = vl.no_ville
  WHERE EXTRACT ( year  FROM date_commande) IN (2017,2018)
    AND EXTRACT ( month FROM date_commande) <= 5
    AND vl.pays IN ('Allemagne','France')
  GROUP BY  vl.pays,
            EXTRACT ( year  FROM date_commande) ,
            EXTRACT ( month FROM date_commande)
)
SELECT pays  ,
       annee ,
       mois  ,
       port  ,
       FIRST_VALUE(port) OVER ( PARTITION BY pays
                 ORDER BY annee,mois       ) AS fv1,
       FIRST_VALUE(port) OVER ( PARTITION BY pays
                 ORDER BY annee,mois
                 ROWS 1 PRECEDING          ) AS fv2,
       LAST_VALUE(port) OVER ( PARTITION BY pays
                 ORDER BY annee,mois
                 ROWS BETWEEN CURRENT ROW
                      AND UNBOUNDED FOLLOWING) AS lv1,
       FIRST_VALUE(port) OVER ( PARTITION BY pays
                 ORDER BY annee,mois
                 ROWS BETWEEN CURRENT ROW
                      AND 1 FOLLOWING) AS lv2
FROM sr_initiale
ORDER BY pays,annee,mois;

WITH sr_initiale (
  SELECT vl.pays                                      ,
         EXTRACT ( year  FROM date_commande) AS annee ,
         EXTRACT ( month FROM date_commande) AS mois  ,
         SUM(port)                           AS port
  FROM COMMANDES CO
          JOIN details_commandes dc
               ON dc.no_commande = co.no_commande
          JOIN acheteurs ac
               ON co.no_acheteur = ac.no_acheteur
          JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
          JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
          JOIN villes vl            ON ad.no_ville    = vl.no_ville
  WHERE EXTRACT ( year  FROM date_commande) IN (2017,2018)
    AND EXTRACT ( month FROM date_commande) <= 5
    AND vl.pays IN ('Allemagne','France')
  GROUP BY  vl.pays,
            EXTRACT ( year  FROM date_commande) ,
            EXTRACT ( month FROM date_commande)
)
SELECT pays  ,
       annee ,
       mois  ,
       port  ,
       FIRST_VALUE(port) OVER ( PARTITION BY pays
                 ORDER BY annee,mois       ) AS fv1,
       FIRST_VALUE(port) OVER ( PARTITION BY pays
                 ORDER BY annee,mois
                 ROWS 1 PRECEDING          ) AS fv2,
       LAST_VALUE(port) OVER ( PARTITION BY pays
                 ORDER BY annee,mois
                 ROWS BETWEEN CURRENT ROW
                      AND UNBOUNDED FOLLOWING) AS lv1,
       FIRST_VALUE(port) OVER ( PARTITION BY pays
                 ORDER BY annee,mois
                 ROWS BETWEEN CURRENT ROW
                      AND 1 FOLLOWING) AS lv2
FROM sr_initiale
ORDER BY pays,annee,mois;

WITH sr_initiale (
  SELECT vl.pays                                      ,
         EXTRACT ( year  FROM date_commande) AS annee ,
         EXTRACT ( month FROM date_commande) AS mois  ,
         SUM(port)                           AS port
  FROM COMMANDES CO
          JOIN details_commandes dc
               ON dc.no_commande = co.no_commande
          JOIN acheteurs ac
               ON co.no_acheteur = ac.no_acheteur
          JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
          JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
          JOIN villes vl            ON ad.no_ville    = vl.no_ville
  WHERE EXTRACT ( year  FROM date_commande) IN (2017,2018)
    AND EXTRACT ( month FROM date_commande) <= 5
    AND vl.pays IN ('Allemagne','France')
  GROUP BY  vl.pays,
            EXTRACT ( year  FROM date_commande) ,
            EXTRACT ( month FROM date_commande)
)
SELECT pays  ,
       annee ,
       mois  ,
       port  ,
       LAG(port,1,-9999) OVER ( PARTITION BY pays
                        ORDER BY annee,mois ) AS lag1,
       LAG(port,3,-9999) OVER ( PARTITION BY pays
                        ORDER BY annee,mois ) AS lag2,
       LEAD(port,1,-9999) OVER ( PARTITION BY pays
                        ORDER BY annee,mois ) AS lead1,
       LEAD(port,3,-9999) OVER ( PARTITION BY pays
                        ORDER BY annee,mois ) AS lead2
FROM sr_initiale
ORDER BY pays,annee,mois;
