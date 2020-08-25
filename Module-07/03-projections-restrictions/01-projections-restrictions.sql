SELECT * FROM categories;
SELECT ca.*, ca.nom_catesparkgorie FROM categories ca;
SELECT fonction FROM employes;

SELECT 0 as c1, -1234567890 as c2, +1234567890 as c3,
           -123.456 as c4, +123.456 as c5, -1E+123 as c6;

SELECT "Bonjour aujourd'hui c'est le :" AS C1, current_date() AS C2 ;
SELECT 'Bonjour aujourd\'hui c\'est le :' AS C1, current_date() AS C2 ;


SELECT * FROM
VALUES  ("Razvan", array(55, 1.81)),
        ("Radu", array(54, 1.81))
        AS data(Prenom, Age_Taille)

SELECT * FROM
VALUES  ("Razvan", array(map("age",55),
          map("taille",1.81), map("poid",100))),
        ("Radu", array(map("age",54),
          map("taille",1.81), map("poid",150)))
        AS data(Prenom, Infos);


spark.sql("""

""").show(truncate=False)




MapType
