from pyspark.sql.functions import expr,col
from pyspark.sql.types     import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

meteoDataFrame  = spark.read.format('csv')\
    .option('sep',';')\
    .option('header','true')\
    .option('nullValue','mq')\
    .option('inferSchema', 'true')\
    .load('donnees/meteo')\
    .cache()

meteoDataFrame.select('numer_sta',
        expr('t  - 273.15').alias('temperature'),
        expr('(t + pres/1000)*vv/1000')
             .alias('calc')).show(3)

schema = StructType([
        StructField('Id'           , StringType() , True),
        StructField('ville'        , StringType() , True),
        StructField('latitude'     , FloatType() , True),
        StructField('longitude'    , FloatType() , True),
        StructField('altitude'     , IntegerType() , True)])

villes  = spark.read.format('csv')   \
      .option('sep',';')                \
      .option('mergeSchema', 'true')    \
      .option('header','true')          \
      .schema(schema)                   \
      .load('/user/spark/donnees/postesSynop.csv')  \
      .cache()

villes.select('*',expr('altitude * 1000').alias('alt')).show(3)
villes.selectExpr('*','altitude * 1000 as alt').show(3)

meteoDataFrame.selectExpr('numer_sta',
        expr('t  - 273.15').alias('temperature'),
        expr('(t + pres/1000)*vv/1000')
             .alias('calc')).show(3)
