val meteoDataFrame  = spark.read.format("csv").
    option("sep",";").
    option("header","true").
    option("nullValue","mq").
    option("inferSchema", "true").
    load("donnees/meteo").
    cache()

meteoDataFrame.printSchema()
meteoDataFrame.col("date")
