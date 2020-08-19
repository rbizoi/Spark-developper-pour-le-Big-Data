import org.apache.spark.sql.functions._

meteo.where("id < 8000").
     select("annee","mois_jour",
             "temperature").
     describe().show()

meteo.where("id < 8000").count()
meteo.count()

meteo.where("id < 8000").
     select("humidite","visibilite","pression").
     describe().show()


meteo.where("id < 8000").
          groupBy("id","annee").
          agg(
                 count("id").alias("nb_villes"),
                 round(avg("temperature"),2).alias("temperature"),
                 round(avg("humidite"),2).alias("humidite"),
                 round(avg("visibilite"),2).alias("visibilite"),
                 round(avg("pression"),2).alias("pression")
          ).show(10)

meteo.where("id < 8000").
               groupBy("id","annee").
               agg(
                      "id"->"count",
                      "temperature"->"avg",
                      "humidite"->"avg"
               ).toDF("id","annee","nb_villes","temperature",
                      "humidite").show(10)

meteo.where("id < 8000").
     groupBy("id","mois").
     agg(round(avg("temperature"),2)).
     orderBy("id","mois").
     show()

meteo.where("id < 8000").
    groupBy("id").
    pivot("mois").
    agg(round(avg("temperature"),2)).
    toDF("id","Jan","Fev","Mar","Avr","Mai",
         "Jun","Jul","Aou","Sep","Oct","Nov","Dec").
    orderBy("id").
    show()
