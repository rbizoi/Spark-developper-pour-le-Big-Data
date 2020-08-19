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

val meteo = meteoDataFrame.select(
                 col("numer_sta"),
                 col("date").substr(0,4).cast("int"),
                 col("date").substr(5,2).cast("int"),
                 col("date").substr(7,2).cast("int"),
                 col("date").substr(5,4),
                 round(col("t") - 273.15,2),
                 col("u") / 100 ,
                 col("vv") / 1000 ,
                 col("pres") / 1000
                 ).
             toDF("id","annee","mois","jour","mois_jour",
                  "temperature","humidite","visibilite","pression").
             cache()

meteo.select("annee","mois","jour","temperature","humidite",
                          "visibilite","pression").show(3)
