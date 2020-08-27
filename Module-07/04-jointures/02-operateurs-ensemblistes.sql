use gest_comm;
use gest_comm;
show tables;

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

SELECT DISTINCT ville FROM clients_pos WHERE pays = 'France';

SELECT ville FROM fournisseurs_pos WHERE pays = 'France';

SELECT DISTINCT ville FROM clients_pos WHERE pays = 'France'
UNION
SELECT          ville FROM fournisseurs_pos WHERE pays = 'France';

SELECT DISTINCT ville FROM clients_pos WHERE pays = 'France'
UNION ALL
SELECT          ville FROM fournisseurs_pos WHERE pays = 'France';

SELECT DISTINCT ville FROM clients_pos WHERE pays = 'France'
INTERSECT
SELECT          ville FROM fournisseurs_pos WHERE pays = 'France';

SELECT DISTINCT ville FROM clients_pos WHERE pays = 'France'
EXCEPT
SELECT          ville FROM fournisseurs_pos WHERE pays = 'France';

SELECT          ville FROM fournisseurs_pos WHERE pays = 'France'
MINUS
SELECT DISTINCT ville FROM clients_pos WHERE pays = 'France';

SELECT DISTINCT ville FROM clients_pos WHERE pays = 'France'
EXCEPT ALL
SELECT          ville FROM fournisseurs_pos WHERE pays = 'France';

SELECT DISTINCT ville FROM clients_pos WHERE pays = 'France'
MINUS ALL
SELECT          ville FROM fournisseurs_pos WHERE pays = 'France';
