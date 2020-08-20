import org.apache.spark.sql.functions._

val meteo = meteoDataFrame.select(
                 col("numer_sta"),
                 to_date(col("date").substr(0,8),"yyyyMMdd"),
                 to_timestamp(col("date").cast("string"),"yyyyMMddHHmmss"),
                 col("date").substr(0,4).cast("int"),
                 col("date").substr(5,2).cast("int"),
                 col("date").substr(7,2).cast("int"),
                 col("date").substr(5,4),
                 round(col("t") - 273.15,2),
                 col("u") / 100 ,
                 col("vv") / 1000 ,
                 col("pres") / 1000,
                 col("rr1")
                 ).
           toDF("id","date","timestamp","annee","mois","jour","mois_jour","temperature",
                "humidite","visibilite","pression","precipitations").
           cache()

meteo.select("date","timestamp","temperature","humidite",
             "visibilite","pression","precipitations").show(3)
