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



spark.sql("""
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
--ORDER BY pays,annee,mois
""").show(120,truncate=False)

spark.sql("""
""").show(120,truncate=False)


spark.sql("""
WITH sr_initiale (
  SELECT vl.pays                                      ,
         vl.ville                                     ,
         EXTRACT ( year  FROM date_commande) AS annee ,
         EXTRACT ( month FROM date_commande) AS mois  ,
         SUM(port)                           AS port
  FROM COMMANDES CO
          JOIN details_commandes dc ON dc.no_commande = co.no_commande
          JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
          JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
          JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
          JOIN villes vl            ON ad.no_ville    = vl.no_ville
  GROUP BY  vl.pays,
            vl.ville,
            EXTRACT ( year  FROM date_commande) ,
            EXTRACT ( month FROM date_commande)
)
SELECT pays  ,
       annee ,
       mois  ,
       port  ,
       SUM(port) OVER (PARTITION BY pays ) AS t_pays,
       SUM(port) OVER (PARTITION BY annee) AS t_annee,
       SUM(port) OVER (PARTITION BY annee,mois ) AS t_annee_mois
FROM sr_initiale
ORDER BY pays,annee,mois;
""").show(120,truncate=False)


spark.sql("""
use gest_comm;
""").show(120,truncate=False)






spark.sql("""
SELECT vl.pays                                      ,
       vl.ville                                     ,
       EXTRACT ( year  FROM date_commande) AS annee ,
       EXTRACT ( month FROM date_commande) AS mois  ,
       SUM(port)                           AS port
FROM COMMANDES CO
        JOIN details_commandes dc ON dc.no_commande = co.no_commande
        JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
        JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
        JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
        JOIN villes vl            ON ad.no_ville    = vl.no_ville
GROUP BY  vl.pays,
          vl.ville,
          EXTRACT ( year  FROM date_commande) ,
          EXTRACT ( month FROM date_commande)
ORDER BY  vl.pays,
          vl.ville,
          EXTRACT ( year  FROM date_commande) ,
          EXTRACT ( month FROM date_commande)
""").show(120,truncate=False)
