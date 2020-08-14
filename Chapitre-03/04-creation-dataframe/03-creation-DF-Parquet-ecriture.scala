val donneesMeteo  = spark.read.format("csv").
      option("sep",";").
      option("mergeSchema", "true").
      option("header","true").
      option("nullValue","mq").
      option("inferSchema", "true").
      load("/user/spark/donnees/meteo.txt")

val fichier = "/user/spark/donnees/meteo_parquet"
val format  = "parquet"
donneesMeteo.write.mode("overwrite").format(format).save(fichier)
:q
