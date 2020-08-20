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

meteo.where("id < 8000").
     groupBy("annee","mois").
     agg(round(sum("pression") / 1000).alias("precipitations")).
     count()

meteo.where("id < 8000").
     groupBy("annee","mois").
     agg(round(sum("pression") / 1000).alias("precipitations")).
     rollup("annee","mois").
     agg( round(sum("precipitations")).alias("precipitations")).
     count()

meteo.where("id < 8000").
     groupBy("annee","mois").
     agg(round(sum("pression") / 1000).alias("precipitations")).
     cube("annee","mois").
     agg( round(sum("precipitations")).alias("precipitations")).
     count()

meteo.where("id < 8000").
     groupBy("annee","mois").
     agg(round(sum("pression") / 1000).alias("precipitations")).
     cube("annee","mois").
     agg( round(sum("precipitations")).alias("precipitations"),
          grouping_id().alias("regroupement")).
     orderBy(col("annee"),col("mois")).
     show(27)

meteo.where("id < 8000").
    groupBy("id","annee","mois").
    agg(round(sum("pression") / 1000).alias("precipitations")).
    rollup("id","annee","mois").
    agg( round(sum("precipitations")).alias("precipitations"),
         grouping("id").alias("r_id*2^2"),
         grouping("annee").alias("r_annee*2^1"),
         grouping("mois").alias("r_mois*2^0"),
         (grouping("id")*4+grouping("annee")*2+grouping("mois")).alias("r_perso"),
         grouping_id().alias("regroupement")
       ).
    orderBy(col("id"),col("annee"),col("mois")).
    where('regroupement > 0).
    show(27)

meteo.where("id < 8000").
    groupBy("id","annee","mois").
    agg(round(sum("pression") / 1000).alias("precipitations")).
    rollup("id","annee","mois").
    agg( grouping("id").alias("r_id*2^2"),
         grouping("annee").alias("r_annee*2^1"),
         grouping("mois").alias("r_mois*2^0"),
         (grouping("id")*4+grouping("annee")*2+grouping("mois")).alias("r_perso"),
         grouping_id().alias("regroupement")
       ).
    orderBy(col("id"),col("annee"),col("mois")).
    where('regroupement > 0).
    show(5)


villes.select('ville,round('altitude,-2).alias("altitude")).
      groupBy("altitude").
      agg(collect_list('ville).alias("ville par altitude")).
      show(truncate=false)
