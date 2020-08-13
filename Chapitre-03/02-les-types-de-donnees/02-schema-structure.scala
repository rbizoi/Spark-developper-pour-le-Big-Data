val parkingStras  = spark.read.format("json").
      option("mergeSchema", "true").
      option("inferSchema", "true").
      load("donnees/json/parking_stras.json").
      cache()

parkingStras.printSchema()
