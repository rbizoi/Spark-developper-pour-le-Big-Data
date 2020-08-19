from pyspark.sql.functions import *
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

meteoDataFrame.columns
meteoDataFrame.printSchema()

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


meteo = meteoDataFrame.select(
                 col('numer_sta'),
                 col('date')[0:4].cast('int') ,
                 col('date')[5:2].cast('int'),
                 col('date')[7:2].cast('int'),
                 col('date')[5:4],
                 round(col('t') - 273.15,2),
                 col('u') / 100 ,
                 col('vv') / 1000 ,
                 col('pres') / 1000
                 ).toDF('id','annee','mois','jour','mois_jour',
                        'temperature','humidite','visibilite','pression')\
             .cache()

meteo.select('annee','mois','jour','temperature','humidite',
             'visibilite','pression').show(3)
