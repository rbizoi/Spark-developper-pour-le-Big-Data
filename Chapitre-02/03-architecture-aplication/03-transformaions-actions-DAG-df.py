from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

from pyspark.sql.functions import avg,round

spark = SparkSession.builder\
        .appName("L'architecture application")\
        .getOrCreate()

spark.conf.get('spark.driver.memory'),\
      spark.conf.get('spark.executor.cores'),\
      spark.conf.get('spark.executor.memory')

meteoDF00  = spark.read.format('csv')         \
      .option('sep',';')                      \
      .option('mergeSchema', 'true')          \
      .option('header','true')                \
      .option('nullValue','mq')               \
      .load('/user/spark/donnees/meteo.txt')  \
      .select('numer_sta', 'date', 't',
              'u', 'vv', 'pres') \
      .cache()

meteoDF01 = meteoDF00.select(
                 meteoDF00['numer_sta'],
                 meteoDF00['date'][0:4].cast('int') ,
                 meteoDF00['date'][5:2].cast('int'),
                 meteoDF00['date'][7:2].cast('int'),
                 meteoDF00['date'][5:4],
                 round(meteoDF00['t'].cast('double') - 273.15,3),
                 meteoDF00['u'].cast('double') / 100 ,
                 meteoDF00['vv'].cast('double') / 1000 ,
                 meteoDF00['pres'].cast('double') / 1000
                 ).toDF('id','annee','mois','jour','mois_jour',
                        'temperature','humidite','visibilite','pression')\
             .cache()

meteoDF02 = meteoDF01.groupBy('id','annee','mois')\
             .agg( round(avg('temperature'),2),
                    round(avg('humidite'),2),
                    round(avg('visibilite'),2),
                    round(avg('pression'),2))\
             .toDF('id','annee','mois','temperature',
                 'humidite','visibilite','pression')\
             .cache()

meteoDF03 = meteoDF02.orderBy('id','annee','mois').cache()

def formatVille(ville) -> StringType():
    return ville.title()

formatVilleUDF = spark.udf.register('formatVille', formatVille)

stationsDF00  = spark.read.format('csv') \
      .option('sep',';')                   \
      .option('mergeSchema', 'true')       \
      .option('header','true')             \
      .option('nullValue','mq')            \
      .option('inferSchema', 'true')          \
      .load('/user/spark/donnees/postesSynop.csv')   \
      .toDF('id','ville','latitude','longitude','altitude')\
      .cache()

stationsDF01 = stationsDF00\
    .withColumn('ville',formatVilleUDF(stationsDF00['ville']))\
    .cache()

stationsDF02 = stationsDF01.filter( stationsDF01.id < '08000').cache()

meteoDF04 = stationsDF02.join( meteoDF03,
               stationsDF02.id == meteoDF03.id, 'inner')\
               .select ('ville','latitude','longitude'
                       ,'altitude','annee','mois','temperature','humidite',
                       'visibilite','pression')\
               .orderBy('ville','annee','mois')\
               .cache()

meteoDF05 = meteoDF04.groupBy('ville','annee')\
             .agg( round(avg('temperature'),2),
                    round(avg('humidite'),2),
                    round(avg('visibilite'),2),
                    round(avg('pression'),2))\
             .toDF('ville','annee','temperature',
                 'humidite','visibilite','pression')\
             .orderBy('ville','annee')\
             .cache()

meteoDF06 = meteoDF05.filter( stationsDF01.ville == 'Strasbourg-Entzheim').cache()

meteoDF03.show(3)

meteoDF06.select('annee','temperature',
                 'humidite','visibilite','pression')\
                 .show(25)
