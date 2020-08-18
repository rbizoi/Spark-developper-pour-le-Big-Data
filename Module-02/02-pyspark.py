donneesStations = spark.read.format("csv").\
         option("sep", ";").option("mergeSchema", "true").\
         option("header","true").option("nullValue","mq").\
         load("file:/home/spark/postesSynop.csv").\
         filter("ID<8000").toDF("Station","Ville",\
         "Latitude","Longitude","Altitude")

donneesStations.printSchema()

donneesStations.show(5)
exit()
