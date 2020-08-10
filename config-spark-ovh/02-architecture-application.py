export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_DRIVER_PYTHON_OPTS=''
pyspark --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 8 \
    --executor-memory 20g

from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

from pyspark.sql.functions import avg,round

spark = SparkSession.builder.\
        appName("L'architecture application").\
        getOrCreate()

spark.conf.get('spark.driver.memory'),\
      spark.conf.get('spark.executor.cores'),\
      spark.conf.get('spark.executor.memory')

def transformLigneMeteo(ligne):
   champs = ligne.split(';')
   return ( str(champs[0]),
            (int(str(champs[1])[0:4]),
            int(str(champs[1])[4:6]),
            int(str(champs[1])[6:8]),
            float(str(champs[7]))      - 273.15,
            float(int(str(champs[9]))  / 100 ),
            float(int(str(champs[10])) / 1000 ),
            float(int(str(champs[20])) / 1000 )) )

def transformLignePoste(ligne):
    champs = ligne.split(';')
    return ( str(champs[0]),
            (str(champs[1]).title(),
            float(champs[2]),
            float(champs[3]),
            int(champs[4])))

donnees00 = spark.sparkContext. \
       textFile('/user/spark/donnees/meteo.txt').\
       persist()

donnees01 = donnees00.filter( lambda ligne :
                      str(ligne)[0:5].isdigit())

donnees02 = donnees01.map(lambda ligne:
                      str(ligne).replace('mq','0'))

donnees03 = donnees02.map(lambda ligne:
                      transformLigneMeteo(ligne))

donnees04 = spark.sparkContext.\
       textFile('/user/spark/donnees/postesSynop.csv').\
       persist()

donnees05 = donnees04.filter( lambda ligne :
                         str(ligne)[0].isdigit() )

donnees06 = donnees05.map( lambda ligne :
                          transformLignePoste(ligne))

donnees07 = donnees06.join(donnees03)

donnees08 = donnees07.sortByKey()

donnees09 = donnees08.map(lambda ligne : tuple([ligne[0]] +
                           [x for x in ligne[1][0]] +
                           [x for x in ligne[1][1]]) )

schema = StructType([
            StructField('Id'           , StringType() , True),
            StructField('ville'        , StringType() , True),
            StructField('latitude'     , StringType() , True),
            StructField('longitude'    , StringType() , True),
            StructField('altitude'     , StringType() , True),
            StructField('annee'        , IntegerType(), True),
            StructField('mois'         , IntegerType(), True),
            StructField('jour'         , IntegerType(), True),
            StructField('temperature'  , FloatType()  , True),
            StructField('humidite'     , FloatType()  , True),
            StructField('visibilite'   , FloatType()  , True),
            StructField('pression'     , FloatType()  , True)])

donnees10 = spark.createDataFrame(donnees09, schema)
donnees11 = donnees10.filter(donnees10.Id < '08000')
donnees12 = donnees11.groupBy('ville').\
                      pivot('mois').\
                      agg(round(avg('temperature'),2)).\
                      sort('ville').\
                      toDF('Ville','Jan','Fev','Mar',
                      'Avr','Mai','Jun','Jul','Aou',
                      'Sep','Oct','Nov','Dec')

#'Janvier','Février','Mars','Avril','Mai','Juin','Juillet','Août','Septembre','Octobre','Novembre','Décembre'

spark.sql('use cours_spark')
spark.sql('DROP TABLE IF EXISTS cours_spark.donneesMeteo12')
donnees12.write.saveAsTable('donneesMeteo12')
spark.sql("select Ville,Jan,Fev,Mar,Aou,Oct,Nov,Dec \
           from cours_spark.donneesMeteo12 \
           order by Ville").show(42)




meteoDF00  = spark.read.format("csv") \
      .option("sep",";")                   \
      .option("mergeSchema", "true")       \
      .option("header","true")             \
      .option("nullValue","mq")            \
      .load("/user/spark/donnees/meteo")   \
      .select('numer_sta', 'date', 't',
              'u', 'vv', 'pres')

meteoDF01 = meteoDF00.select(
                 meteoDataFrame['numer_sta'],
                 meteoDataFrame['date'][0:4].cast('int') ,
                 meteoDataFrame['date'][5:2].cast('int'),
                 meteoDataFrame['date'][7:2].cast('int'),
                 meteoDataFrame['date'][5:4],
                 round(meteoDataFrame['t'].cast('double') - 273.15,3),
                 meteoDataFrame['u'].cast('double') / 100 ,
                 meteoDataFrame['vv'].cast('double') / 1000 ,
                 meteoDataFrame['pres'].cast('double') / 1000
                 ).toDF('id','annee','mois','jour','mois_jour',
                 'temperature','humidite','visibilite','pression')


def formatVille(ville) -> StringType():
    return ville.title()

formatVilleUDF = spark.udf.register("formatVille", formatVille)


stationsDF00  = spark.read.format("csv") \
      .option("sep",";")                   \
      .option("mergeSchema", "true")       \
      .option("header","true")             \
      .option("nullValue","mq")            \
      .load("/user/spark/donnees/postesSynop.csv")   \
      .toDF('id','ville','latitude','longitude','altitude')


stationsDF00.\
    withColumn("ville",formatVilleUDF(stationsDF00['ville'])).\
    withColumn("latitude",stationsDF00['latitude'].cast('double')).\
    withColumn("longitude",stationsDF00['longitude'].cast('double')).\
    withColumn("altitude",stationsDF00['altitude'].cast('int')).\
    show()




meteoDF01 = stationsDF00.select(
                 meteoDataFrame['id'],
                 meteoDataFrame['ville'][0:4].cast('int') ,
                 meteoDataFrame['date'][5:2].cast('int'),
                 meteoDataFrame['date'][7:2].cast('int'),
                 meteoDataFrame['date'][5:4],
                 round(meteoDataFrame['t'].cast('double') - 273.15,3),
                 meteoDataFrame['u'].cast('double') / 100 ,
                 meteoDataFrame['vv'].cast('double') / 1000 ,
                 meteoDataFrame['pres'].cast('double') / 1000
                 ).toDF('id','annee','mois','jour','mois_jour',
                 'temperature','humidite','visibilite','pression')


textFile('/user/spark/donnees/postesSynop.csv').\
