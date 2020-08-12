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

stationsDF01  = spark.read.format('csv') \
      .option('sep',';')                   \
      .option('mergeSchema', 'true')       \
      .option('header','true')             \
      .option('nullValue','mq')            \
      .option('inferSchema', 'true')          \
      .load('/user/spark/donnees/postesSynop.csv')   \
      .toDF('id','ville','latitude','longitude','altitude')\
      .cache()
