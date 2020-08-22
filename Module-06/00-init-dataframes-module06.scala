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
                 col("pres") / 1000,
                 col("rr1")
                 ).
           toDF("id","annee","mois","jour","mois_jour","temperature",
                "humidite","visibilite","pression","precipitations").
           cache()

meteo.select("annee","mois","jour","temperature","humidite",
                          "visibilite","pression","precipitations").show(3)


val meteoFance = meteo.where("id < 8000").
             join( villes.withColumnRenamed("Id", "id"),"id").
             select(initcap(regexp_replace('ville,"-"," ")).alias("ville"),
                     'annee,'mois,'jour,'temperature,
                     'humidite,'visibilite,'pression,'precipitations)

val data = Array( ("Ajaccio"     ,"dfa" ),
                  ("Angers"      ,"dfa" ),
                  ("Angoulème"   ,"dfa" ),
                  ("Besançon"    ,"dfa" ),
                  ("Biarritz"    ,"dfa" ),
                  ("Bordeaux"    ,"dfa" ),
                  ("Brest"       ,"dfa" ),
                  ("Caen"        ,"dfa" ),
                  ("Clermont-Fd" ,"dfa" ),
                  ("Dijon"       ,"dfa" ),
                  ("Embrun"      ,"dfa" ),
                  ("Grenoble"    ,"dfa" ),
                  ("Lille"       ,"dfa" ),
                  ("Limoges"     ,"dfa" ),
                  ("Lyon"        ,"dfa" ),
                  ("Marseille"   ,"dfa" ),
                  ("Montpellier" ,"dfa" ),
                  ("Nancy"       ,"dfa" ),
                  ("Nantes"      ,"dfa" ),
                  ("Nice"        ,"dfa" ),
                  ("Nîmes"       ,"dfa" ),
                  ("Orléans"     ,"dfa" ),
                  ("Paris"       ,"dfa" ))

val dfa = spark.sparkContext.parallelize(data).toDF("ville","valeur")

val data = Array( ("Nancy"       ,"dfb" ),
                  ("Nantes"      ,"dfb" ),
                  ("Nice"        ,"dfb" ),
                  ("Nîmes"       ,"dfb" ),
                  ("Orléans"     ,"dfb" ),
                  ("Paris"       ,"dfb" ),
                  ("Perpignan"   ,"dfb" ),
                  ("Poitiers"    ,"dfb" ),
                  ("Reims"       ,"dfb" ),
                  ("Rennes"      ,"dfb" ),
                  ("Rouen"       ,"dfb" ),
                  ("St-Quentin"  ,"dfb" ),
                  ("Strasbourg"  ,"dfb" ),
                  ("Toulon"      ,"dfb" ),
                  ("Toulouse"    ,"dfb" ),
                  ("Tours"       ,"dfb" ),
                  ("Vichy"       ,"dfb" ))

val dfb = spark.sparkContext.parallelize(data).toDF("ville","valeur")
