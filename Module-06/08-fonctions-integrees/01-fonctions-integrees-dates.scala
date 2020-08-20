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

meteo.select(date_format('date,
     "d dd DDD M MMM MMMM yy yyyy")).show(3,false)

meteo.sample(0.3).
      orderBy(desc("timestamp")).
      select(date_format('timestamp,"h H m s S")).
      show(10,false)

meteo.sample(0.3).
      orderBy(desc("timestamp")).
      select(date_format('date,"q Q E F")).
      show(3,false)


meteo.select('date,
              date_add('date,1).alias("demain"),
              date_add('date,-1).alias("hier"),
              add_months('date,1).alias("mois dernier"),
              add_months('date,-1).alias("mois prochain"),
              ).show(1)


meteo.select('date,
              next_day('date,"Wed").alias("prochain mercredi"),
              last_day('date).alias("dernier jour du mois"),
              dayofmonth('date).alias("jour du mois"),
              dayofweek('date).alias("jour de la semaine"),
              dayofyear('date).alias("jour de l'année"),
            ).show(1)


meteo.select('date,
              next_day('date,"Wed").alias("prochain mercredi"),
              last_day('date).alias("dernier jour du mois"),
            ).show(1)

meteo.orderBy(desc("timestamp")).
      select('timestamp,
              date_trunc ("year",'date).cast("date").alias("y"),
              date_trunc ("month",'date).cast("date").alias("m"),
              dayofyear ('timestamp).alias("dy"),
              dayofmonth('timestamp).alias("dm"),
              dayofweek ('timestamp).alias("dw"),
              quarter   ('timestamp).alias("q"),
              month     ('timestamp).alias("m"),
              hour      ('timestamp).alias("h"),
            ).show(1)
