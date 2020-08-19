import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val meteoDataFrame  = spark.read.format("csv").
    option("sep",";").
    option("header","true").
    option("nullValue","mq").
    option("inferSchema", "true").
    load("donnees/meteo").
    cache()
meteoDataFrame.columns
meteoDataFrame.printSchema()

meteoDataFrame.select('numer_sta,
        expr("t  - 273.15").alias("temperature"),
        expr("(t + pres/1000)*vv/1000")
             .alias("calc")).show(3)

meteoDataFrame.selectExpr("*"," t  - 273.15 as temperature")).show(3)

val schema = StructType(
   StructField("Id"        , StringType , true)::
   StructField("ville"     , StringType , true)::
   StructField("latitude"  , FloatType  , true)::
   StructField("longitude" , FloatType  , true)::
   StructField("altitude"  , IntegerType, true)::Nil)

val villes  = spark.read.format("csv").
             option("sep",";").
             option("mergeSchema", "true").
             option("header","true").
             schema(schema).
             load("/user/spark/donnees/postesSynop.csv").
             cache()

villes.select('Id,'ville,'latitude,
              'longitude,'altitude,
              expr("altitude * 1000").alias("alt")).show(3)

villes.selectExpr("*","altitude * 1000 as alt").show(3)

villes.drop("Id", "latitude", "longitude").show(3)



val meteo = meteoDataFrame.sample(0.3).select(
                 col("numer_sta"),
                 col("date").cast("string").substr(0,4).cast("int"),
                 col("date").cast("string").substr(5,2).cast("int"),
                 col("date").cast("string").substr(7,2).cast("int"),
                 col("date").cast("string").substr(5,4),
                 col("t") - 273.15,
                 col("u") / 100 ,
                 col("vv") / 1000 ,
                 col("pres") / 1000
                 ).
             toDF("id","annee","mois","jour","mois_jour",
                  "temperature","humidite","visibilite","pression").
             cache()

meteo.select("annee","mois","jour","temperature","humidite",
                          "visibilite","pression").show(3)

meteo.where("id < 8000").
     select("annee","mois_jour",
             "temperature").
     describe().show()
