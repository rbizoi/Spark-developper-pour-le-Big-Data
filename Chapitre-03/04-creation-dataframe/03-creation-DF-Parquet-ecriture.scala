val donneesMeteo  = spark.read.format("csv").
      option("sep",";").
      option("mergeSchema", "true").
      option("header","true").
      option("nullValue","mq").
      option("inferSchema", "true").
      load("/user/spark/donnees/meteo.txt").
      cache()

fichier = "/user/spark/donnees/json/parking_stras_json"
format  = "json"
donneesMeteo.write.mode("overwrite").format(format).save(fichier)
:q
