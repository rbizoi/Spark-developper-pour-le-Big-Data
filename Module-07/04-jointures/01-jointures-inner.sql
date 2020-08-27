use gest_comm;
use gest_comm;
show tables;

SELECT count(*) FROM commandes;
SELECT count(*) FROM vendeurs;
SELECT count(*) FROM commandes CROSS JOIN  vendeurs;

SELECT count(*)
FROM commandes as co
INNER JOIN vendeurs as vn
ON co.no_vendeur = vn.no_vendeur;

SELECT  EXTRACT ( year from date_commande)  as annee,
        EXTRACT ( month from date_commande) as mois,
        nom_produit                         as produit,
        SUM(dc.port)                        as frais,
        SUM(dc.quantite)                    as quantites,
        SUM(dc.quantite*dc.prix_unitaire)   as ca
        FROM magasins  mg
        INNER JOIN adresses  ad ON ad.no_adresse  = mg.no_adresse
        INNER JOIN villes    vl ON vl.no_ville    = ad.no_ville
        AND vl.ville   = 'Paris'
        INNER JOIN acheteurs ac ON ac.no_magasin  = mg.no_magasin
        INNER JOIN commandes co ON ac.no_acheteur = co.no_acheteur
        INNER JOIN details_commandes dc ON co.no_commande = dc.no_commande
        INNER JOIN produits  pr ON dc.ref_produit  = pr.ref_produit
        AND pr.code_categorie = 1
        AND pr.no_fournisseur = 1
GROUP BY  EXTRACT ( year from date_commande),
          EXTRACT ( month from date_commande),
          nom_produit
ORDER BY  EXTRACT ( year from date_commande),
          EXTRACT ( month from date_commande),
          nom_produit ;

SELECT  vp.nom  vp,
        mg.nom  manager,
        em.nom  employe
FROM employes em
      JOIN employes mg ON em.rend_compte = mg.no_employe
      JOIN employes vp ON mg.rend_compte = vp.no_employe
ORDER BY vp, manager, employe;


CREATE OR REPLACE VIEW clients_pos
AS
SELECT cl.societe AS client,
      vl.ville,
      vl.pays
FROM CLIENTS CL
      JOIN magasins ma ON ma.code_client = cl.code_client
                          AND ma.type_adresse = 0
      JOIN adresses ad ON ad.no_adresse = ma.no_adresse
      JOIN villes   vl ON vl.no_ville = ad.no_ville;

CREATE OR REPLACE VIEW fournisseurs_pos
AS
SELECT fr.societe AS fournisseur,
      vl.ville,
      vl.pays
FROM fournisseurs fr
      JOIN adresses ad ON ad.no_adresse = fr.no_adresse
      JOIN villes   vl ON vl.no_ville = ad.no_ville;

SELECT COALESCE(c.client     ,'------------------') AS client,
       COALESCE(f.fournisseur,'------------------') AS fournisseur
FROM clients_pos c INNER JOIN fournisseurs_pos f
     ON( c.ville = f.ville AND c.pays = f.pays)
WHERE COALESCE(c.pays,f.pays) = 'France';

SELECT COALESCE(c.client     ,'------------------') AS client,
       COALESCE(f.fournisseur,'------------------') AS fournisseur
FROM clients_pos c LEFT OUTER JOIN fournisseurs_pos f
     ON( c.ville = f.ville AND c.pays = f.pays)
WHERE COALESCE(c.pays,f.pays) = 'France';

SELECT COALESCE(c.client     ,'------------------') AS client,
       COALESCE(f.fournisseur,'------------------') AS fournisseur
FROM clients_pos c RIGHT OUTER JOIN fournisseurs_pos f
     ON( c.ville = f.ville AND c.pays = f.pays)
WHERE COALESCE(c.pays,f.pays) = 'France';

SELECT COALESCE(c.client     ,'------------------') AS client,
       COALESCE(f.fournisseur,'------------------') AS fournisseur
FROM clients_pos c FULL OUTER JOIN fournisseurs_pos f
     ON( c.ville = f.ville AND c.pays = f.pays)
WHERE COALESCE(c.pays,f.pays) = 'France';
