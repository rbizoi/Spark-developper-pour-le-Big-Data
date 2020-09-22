from pyspark.sql import SparkSession

spark = SparkSession.builder\
              .enableHiveSupport()\
              .getOrCreate()

donnees = spark.sql('select * from parquet.`donnees/meteoFrance`').cache()
donnees.createOrReplaceGlobalTempView('GVmeteoFrance')
donnees.createOrReplaceTempView('TVmeteoFrance')

spark.sql("""select  ville,mois,jour,temperature,humidite,
           visibilite,pression from TVmeteoFrance""").show(3)
spark.sql("""select  ville,mois,jour,temperature,humidite,
           visibilite,pression from global_temp.GVmeteoFrance""").show(3)



autreSession = spark.newSession()
autreSession.sql("""select  ville,mois,jour,temperature,humidite,
           visibilite,pression from global_temp.GVmeteoFrance""").show(3)
autreSession.sql("""select  ville,mois,jour,temperature,humidite,
           visibilite,pression from TVmeteoFrance""").show(3)

donnees.write\
        .mode('overwrite')\
        .format('parquet')\
        .saveAsTable('meteoFrance')
