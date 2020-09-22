import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

meteoDataFrame.columns
meteoDataFrame.printSchema()

meteoDataFrame.select('numer_sta,
        expr("t  - 273.15").alias("temperature"),
        expr("(t + pres/1000)*vv/1000")
             .alias("calc")).show(3)

meteoDataFrame.selectExpr("*"," t  - 273.15 as temperature")).show(3)

villes.select('Id,'ville,'latitude,
              'longitude,'altitude,
              expr("altitude * 1000").alias("alt")).show(3)

villes.selectExpr("*","altitude * 1000 as alt").show(3)

villes.drop("Id", "latitude", "longitude").show(3)



meteo.select("annee","mois","jour","temperature","humidite",
                          "visibilite","pression").show(3)

meteo.where("id < 8000").
     select("annee","mois_jour",
             "temperature").
     describe().show()
