import org.apache.spark.sql.functions._

val meteoFance = meteo.where("id < 8000").
             join( villes.withColumnRenamed("Id", "id"),"id").
             select(initcap(regexp_replace('ville,"-"," ")).alias("ville"),
                     'annee,'mois,'jour,'temperature,
                     'humidite,'visibilite,'pression,'precipitations)

meteoFance.selectExpr("ville","annee","mois","jour","temperature as t",
               "humidite as h","visibilite as v",
               "pression as p","precipitations as e").show()
