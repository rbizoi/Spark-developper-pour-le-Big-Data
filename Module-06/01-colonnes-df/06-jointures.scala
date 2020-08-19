import org.apache.spark.sql.functions._

meteo.join(villes,
       meteo.col("id") === villes.col("Id")).
       select("ville","annee","mois_jour",
               "temperature","precipitations").
       show(10)

meteo.join(villes,
       meteo.col("id").eqNullSafe(villes.col("Id"))).
       select("ville","annee","mois_jour",
               "temperature","precipitations").
       show(10)

meteo.join(villes,
       meteo.col("id").equalTo(villes.col("Id"))).
       select("ville","annee","mois_jour",
               "temperature","precipitations").
       show(10)

meteo.join(villes.withColumnRenamed("Id", "id"),"id").
      select("ville","annee","mois_jour",
              "temperature","precipitations").
      show(10)
