meteoDataFrame  = spark.read.format("csv")\
    .option("sep",";")\
    .option("header","true")\
    .option("nullValue","mq")\
    .option("inferSchema", "true")\
    .load("donnees/meteo")
meteoDataFrame.select("numer_sta","date","t").show(3)
