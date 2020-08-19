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
