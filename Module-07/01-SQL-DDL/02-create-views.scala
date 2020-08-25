import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.
              enableHiveSupport().
              getOrCreate()

val donnees = spark.sql("select * from parquet.`donnees/meteoFrance`")
donnees.createOrReplaceGlobalTempView("GVmeteoFrance")
donnees.createOrReplaceTempView("TVmeteoFrance")

spark.sql("select  ville,mois,jour,temperature,humidite,"+
           "visibilite,pression from TVmeteoFrance").show(3)
spark.sql("select  ville,mois,jour,temperature,humidite,"+
           "visibilite,pression from global_temp.GVmeteoFrance").show(3)

donnees.write.
        mode("overwrite").
        format("parquet").
        saveAsTable("meteoFrance")
