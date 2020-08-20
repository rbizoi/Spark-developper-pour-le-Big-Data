import org.apache.spark.sql.functions._


meteo.where("id < 8000").count()
meteo.where("id < 8000").
     select("annee","temperature", "humidite",
             "visibilite", "pression","precipitations").
     describe().
     show()

meteo.where("id < 8000").count()
meteo.where("id < 8000").
     where(col("temperature").isNotNull).
     where(col("humidite").isNotNull ).
     where(col("visibilite").isNotNull ).
     where(col("pression").isNotNull ).
     where(col("precipitations").isNotNull ).
     count()

meteo.where('id < 8000).count() - meteo.where('id < 8000).
     where('temperature.isNotNull).
     where('humidite.isNotNull ).
     where('visibilite.isNotNull ).
     where('pression.isNotNull ).
     where('precipitations.isNotNull ).
     count()

meteo.where('id < 8000).
    where('temperature.isNotNull).
    where('humidite.isNotNull ).
    where('visibilite.isNotNull ).
    where('pression.isNotNull ).
    count()

meteo.where('id < 8000).
      na.fill(0 ,Seq("precipitations")).
      na.drop().
      count()

val meteoNotNull =  meteo.where('id < 8000).
      na.fill(0 ,Seq("precipitations")).
      na.drop()
meteo.show()
meteoNotNull.show()

meteo.where("temperature is Null").count()
meteo.filter(col("temperature").isNull).count()
meteo.filter(col("temperature").isNull).count()
meteo.filter('temperature.isNull).count()
meteo.filter($"temperature".isNull).count()
meteo.where($"temperature".isNull).count()

meteo.where("humidite is Null").count()
meteo.filter(col("humidite").isNull).count()
meteo.filter(col("humidite").isNull).count()
meteo.filter('humidite.isNull).count()
meteo.filter($"humidite".isNull).count()
meteo.where($"humidite".isNull).count()

meteo.where("visibilite is Null").count()
meteo.filter(col("visibilite").isNull).count()
meteo.filter(col("visibilite").isNull).count()
meteo.filter('visibilite.isNull).count()
meteo.filter($"visibilite".isNull).count()
meteo.where($"visibilite".isNull).count()

meteo.where("pression is Null").count()
meteo.filter(col("pression").isNull).count()
meteo.filter(col("pression").isNull).count()
meteo.filter('pression.isNull).count()
meteo.filter($"pression".isNull).count()
meteo.where($"pression".isNull).count()

meteo.where("precipitations is Null").count()
meteo.filter(col("precipitations").isNull).count()
meteo.filter(col("precipitations").isNull).count()
meteo.filter('precipitations.isNull).count()
meteo.filter($"precipitations".isNull).count()
meteo.where($"precipitations".isNull).count()


meteo.na.fill(0 ,"precipitations").na.drop().show()
