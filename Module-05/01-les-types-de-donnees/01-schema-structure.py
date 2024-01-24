from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

from pyspark.sql.functions import avg,round

customSchemaMeteo = StructType([
            StructField('Id'           , StringType() , True),
            StructField('ville'        , StringType() , True),
            StructField('latitude'     , FloatType()  , True),
            StructField('longitude'    , FloatType()  , True),
            StructField('altitude'     , IntegerType(), True)])

stationsDF00  = spark.read.format('csv')   \
      .option('sep',';')                   \
      .option('mergeSchema', 'true')       \
      .option('header','true')             \
      .option('nullValue','mq')            \
      .schema(customSchemaMeteo)           \
      .load('/user/spark/donnees/postesSynop.csv')   \
      .cache()
stationsDF00.printSchema()
stationsDF01  = spark.read.format('csv') \
      .option('sep',';')                   \
      .option('mergeSchema', 'true')       \
      .option('header','true')             \
      .option('nullValue','mq')            \
      .option('inferSchema', 'true')          \
      .load('/user/spark/donnees/postesSynop.csv')   \
      .toDF('id','ville','latitude','longitude','altitude')\
      .cache()
stationsDF01.printSchema()

customSchemaMeteo = "id STRING, ville STRING, latitude FLOAT, longitude FLOAT, altitude INT"
stationsDF02  = spark.read.format('csv')   \
      .option('sep',';')                   \
      .option('mergeSchema', 'true')       \
      .option('header','true')             \
      .option('nullValue','mq')            \
      .schema(customSchemaMeteo)           \
      .load('/user/spark/donnees/postesSynop.csv')   \
      .cache()
stationsDF02.printSchema()
