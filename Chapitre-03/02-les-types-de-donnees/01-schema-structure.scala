val donneesMeteo  = spark.read.format("csv").
      option("sep",";").
      option("mergeSchema", "true").
      option("header","true").
      option("nullValue","mq").
      option("inferSchema", "true").
      load("/user/spark/donnees/meteo").
      cache()

donneesMeteo.printSchema()
