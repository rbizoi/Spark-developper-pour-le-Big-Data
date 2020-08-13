val parkingStras  = spark.read.format("json").
      option("mergeSchema", "true").
      option("inferSchema", "true").
      load("/user/spark/donnees/parking_stras.json").
      cache()

parkingStras.printSchema()
