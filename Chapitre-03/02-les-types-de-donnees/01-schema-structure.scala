import org.apache.spark.sql.types.{ StructType, StructField,
                                    FloatType , IntegerType,
                                    StringType }

val customSchemaMeteo = StructType(
    StructField("id"        , StringType , true)::
    StructField("ville"     , StringType , true)::
    StructField("latitude"  , FloatType  , true)::
    StructField("longitude" , FloatType  , true)::
    StructField("altitude"  , IntegerType, true)::Nil )

val stationsDF00  = spark.read.format("csv").
      option("sep",";").
      option("mergeSchema", "true").
      option("header","true").
      option("nullValue","mq").
      schema(customSchemaMeteo).
      load("/user/spark/donnees/postesSynop.csv").
      cache()
stationsDF00.printSchema()
val stationsDF01  = spark.read.format("csv").
      option("sep",";").
      option("mergeSchema", "true").
      option("header","true").
      option("nullValue","mq").
      option("inferSchema", "true").
      load("/user/spark/donnees/postesSynop.csv").
      toDF("id","ville","latitude","longitude","altitude").
      cache()
stationsDF01.printSchema()

val  customSchemaMeteo = "id STRING, ville STRING, latitude FLOAT, longitude FLOAT, altitude INT"
val stationsDF02  = spark.read.format("csv").
      option("sep",";").
      option("mergeSchema", "true").
      option("header","true").
      option("nullValue","mq").
      schema(customSchemaMeteo).
      load("/user/spark/donnees/postesSynop.csv").
      cache()
stationsDF02.printSchema()
