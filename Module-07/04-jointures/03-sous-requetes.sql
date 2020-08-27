use gest_comm;
show tables;

SELECT AVG(dc.port) port
FROM COMMANDES CO
        JOIN details_commandes dc ON dc.no_commande = co.no_commande
        JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
        JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
        JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
        JOIN villes vl            ON ad.no_ville    = vl.no_ville
                                      AND vl.pays = 'France'  ;



WITH sr1 as ( SELECT AVG(dc.port) port
  FROM COMMANDES CO
          JOIN details_commandes dc ON dc.no_commande = co.no_commande
          JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
          JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
          JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
          JOIN villes vl            ON ad.no_ville    = vl.no_ville
                                        AND vl.pays = 'France'      )
SELECT ac.no_magasin ,
       vl.ville,
       vl.pays,
       dc.ref_produit,
       AVG(dc.port) moyPort
FROM commandes co
        JOIN details_commandes dc ON dc.no_commande = co.no_commande
        JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
        JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
        JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
        JOIN villes vl            ON ad.no_ville    = vl.no_ville
                                      AND vl.pays = 'France'
WHERE   dc.port > (SELECT port*1.3 FROM sr1)
GROUP BY ac.no_magasin ,
         vl.ville,
         vl.pays,
         dc.ref_produit
ORDER BY moyPort DESC
LIMIT 10;

WITH sr1 as ( SELECT AVG(dc.port) port
FROM COMMANDES CO
        JOIN details_commandes dc ON dc.no_commande = co.no_commande
        JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
        JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
        JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
        JOIN villes vl            ON ad.no_ville    = vl.no_ville
                                      AND vl.pays = 'France'      )
SELECT ac.no_magasin ,
       vl.ville,
       vl.pays,
       dc.ref_produit,
       AVG(dc.port) moyPort
FROM commandes co
        JOIN details_commandes dc ON dc.no_commande = co.no_commande
        JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
        JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
        JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
        JOIN villes vl            ON ad.no_ville    = vl.no_ville
                                      AND vl.pays = 'France'
GROUP BY ac.no_magasin ,
         vl.ville,
         vl.pays,
         dc.ref_produit
HAVING   AVG(dc.port) > (SELECT port*1.1 FROM sr1)
ORDER BY moyPort DESC;


SELECT ac.no_magasin ,
       vl.ville,
       vl.pays,
       dc.ref_produit,
       AVG(dc.port) moyPort
FROM commandes co
        JOIN details_commandes dc ON dc.no_commande = co.no_commande
        JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
        JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
        JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
        JOIN villes vl            ON ad.no_ville    = vl.no_ville
                                      AND vl.pays = 'France'
GROUP BY ac.no_magasin ,
         vl.ville,
         vl.pays,
         dc.ref_produit
HAVING   AVG(dc.port) > (SELECT port*1.1 FROM sr1)
ORDER BY moyPort DESC;



SELECT fa.no_commande
FROM factures fa JOIN relances re
     ON fa.no_facture = re.no_facture
GROUP BY fa.no_commande
HAVING COUNT( re.no_facture) > 2
LIMIT 10



WITH sr1 (
      SELECT fa.no_commande
      FROM factures fa JOIN relances re
           ON fa.no_facture = re.no_facture
      GROUP BY fa.no_commande
      HAVING COUNT( re.no_facture) > 2
)
SELECT ac.no_magasin ,
       vl.ville,
       vl.pays,
       co.no_commande,
       COUNT(dc.ref_produit) nb_produits
FROM commandes co
        JOIN details_commandes dc ON dc.no_commande = co.no_commande
        JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
        JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
        JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
        JOIN villes vl            ON ad.no_ville    = vl.no_ville
                                      AND vl.pays = 'France'
WHERE co.no_commande in (SELECT no_commande from sr1)
GROUP BY ac.no_magasin ,
         vl.ville,vl.pays,
         co.no_commande
HAVING COUNT(dc.ref_produit) > 45  ;


WITH sr1 AS (
    SELECT vl.ville,
           vl.pays,
           AVG(dc.port) AS port
    FROM commandes co
                 JOIN details_commandes dc ON dc.no_commande = co.no_commande
                 JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
                 JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
                 JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
                 JOIN villes vl            ON ad.no_ville    = vl.no_ville
    GROUP BY vl.ville,
             vl.pays
  )
SELECT cl.societe            AS client,
       vl.ville,
       vl.pays,
       co.no_commande,
       COUNT(dc.ref_produit) AS nbp,
       ROUND(AVG(dc.port),2) AS port
FROM commandes co
     JOIN details_commandes dc ON dc.no_commande = co.no_commande
     JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
     JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
     JOIN clients cl           ON mg.code_client  = cl.code_client
     JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
     JOIN villes vl            ON ad.no_ville    = vl.no_ville
WHERE dc.port > ( SELECT AVG(port)*1.3 FROM sr1
                   WHERE sr1.ville = vl.ville
                     AND sr1.pays = vl.pays )
GROUP BY cl.societe,
         vl.ville,
         vl.pays,
         co.no_commande
HAVING COUNT(dc.ref_produit) > 48 ;

WITH sat_p AS  (
  SELECT cl.societe                              AS client,
         EXTRACT ( YEAR FROM  co.date_commande)  AS annee,
         EXTRACT ( MONTH FROM co.date_commande)  AS mois,
         SUM(dc.port)                            AS port
  FROM commandes co
       JOIN details_commandes dc ON dc.no_commande = co.no_commande
       JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
       JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
       JOIN clients cl           ON mg.code_client = cl.code_client
  GROUP BY cl.societe                            ,
         EXTRACT ( YEAR FROM  co.date_commande)  ,
         EXTRACT ( MONTH FROM co.date_commande) )
SELECT client, annee, mois, port
FROM sat_p sr1
WHERE port IN ( SELECT MAX(port) FROM sat_p sr2
                WHERE sr1.annee = sr2.annee
                  AND sr1.mois = sr2.mois
                GROUP BY annee, mois            )
ORDER BY annee,mois;

SELECT ac.no_magasin ,
       vl.ville,
       vl.pays,
       co.no_commande,
       COUNT(dc.ref_produit) nb_produits
FROM commandes co
        JOIN details_commandes dc ON dc.no_commande = co.no_commande
        JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
        JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
        JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
        JOIN villes vl            ON ad.no_ville    = vl.no_ville
                                  AND vl.pays = 'France'
WHERE EXISTS (  SELECT NO_COMMANDE
                FROM  factures fa JOIN relances re
                      ON fa.no_facture = re.no_facture
                WHERE fa.no_commande = co.no_commande
                GROUP BY fa.no_commande
                HAVING COUNT( fa.no_facture) > 2 )
GROUP BY ac.no_magasin ,
         vl.ville,
         vl.pays,
         co.no_commande
HAVING COUNT(dc.ref_produit) > 45  ;


SELECT ac.no_magasin ,
       vl.ville,
       vl.pays,
       co.no_commande,
       COUNT(dc.ref_produit) nb_produits
FROM commandes co
        JOIN details_commandes dc ON dc.no_commande = co.no_commande
        JOIN acheteurs ac         ON co.no_acheteur = ac.no_acheteur
        JOIN magasins mg          ON ac.no_magasin  = mg.no_magasin
        JOIN adresses ad          ON mg.no_adresse  = ad.no_adresse
        JOIN villes vl            ON ad.no_ville    = vl.no_ville
                                  AND vl.pays = 'France'
        JOIN (  SELECT NO_COMMANDE
                FROM  factures fa JOIN relances re
                      ON fa.no_facture = re.no_facture
                GROUP BY fa.no_commande
                HAVING COUNT( fa.no_facture) > 2 ) sr1
                ON sr1.no_commande = co.no_commande
GROUP BY ac.no_magasin ,
         vl.ville,
         vl.pays,
         co.no_commande
HAVING COUNT(dc.ref_produit) > 45  ;
