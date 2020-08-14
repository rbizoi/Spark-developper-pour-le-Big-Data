from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

meteoDF00  = spark.read.format('csv')         \
      .option('sep',';')                      \
      .option('mergeSchema', 'true')          \
      .option('header','true')                \
      .option('nullValue','mq')               \
      .option("inferSchema", "true")          \
      .load('/user/spark/donnees/meteo.txt')  \
      .select('numer_sta', 'date', 't',
              'u', 'vv', 'pres') \
      .cache()

meteoDF00.printSchema()
meteoDF00.show(3)

schema = StructType([
        StructField('Id'           , StringType() , True),
        StructField('ville'        , StringType() , True),
        StructField('latitude'     , FloatType() , True),
        StructField('longitude'    , FloatType() , True),
        StructField('altitude'     , IntegerType() , True)])

postesDF00  = spark.read.format('csv')   \
      .option('sep',';')                \
      .option('mergeSchema', 'true')    \
      .option('header','true')          \
      .schema(schema)                   \
      .load('/user/spark/donnees/postesSynop.csv')  \
      .cache()

postesDF00.printSchema()
postesDF00.select('ville','altitude').show(3)

fichier = '/user/spark/donnees/postesSynop_csv'
format  = 'csv'
postesDF00.write.mode('overwrite').format(format).save(fichier)
