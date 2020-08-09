export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_DRIVER_PYTHON_OPTS=''
pyspark --master spark://jupiter.olimp.fr:7077

spark.conf.set('spark.default.parallelism',1)
spark.conf.get('spark.driver.memory'),\
      spark.conf.get('spark.executor.cores'),\
      spark.conf.get('spark.executor.memory'),\
      spark.conf.get('spark.default.parallelism')

from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

from pyspark.sql.functions import avg,round

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
            (str(champs[1]),
            float(champs[2]),
            float(champs[3]),
            int(champs[4])))

donnees00 = spark.sparkContext. \
       textFile('/user/spark/donnees/meteo').\
       persist()

donnees01 = donnees00.filter( lambda ligne :
                      str(ligne)[0:5].isdigit()).\
                      persist()

donnees02 = donnees01.map(lambda ligne:
                      str(ligne).replace('mq','0')).\
                      persist()

donnees03 = donnees02.map(lambda ligne:
                      transformLigneMeteo(ligne)).\
                      persist()

donnees04 = spark.sparkContext.\
       textFile('/user/spark/donnees/postesSynop.csv').\
       persist()

donnees05 = donnees04.filter( lambda ligne :
                         str(ligne)[0].isdigit() ).\
                         persist()

donnees06 = donnees05.map( lambda ligne :
                          transformLignePoste(ligne)).\
                          persist()

donnees07 = donnees06.join(donnees03).persist()

donnees00.id(),\
donnees01.id(),\
donnees02.id(),\
donnees03.id(),\
donnees04.id(),\
donnees05.id(),\
donnees06.id(),\
donnees07.id()

donnees08 = donnees07.sortByKey().persist()

donnees09 = donnees08.map(lambda ligne : tuple([ligne[0]] +
                           [x for x in ligne[1][0]] +
                           [x for x in ligne[1][1]]) ).persist()

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

donneesMeteo = spark.createDataFrame(donnees09, schema).cache()
donneesMeteo12 = donneesMeteo.groupBy('ville').\
                      pivot('mois').\
                      agg(round(avg('temperature'),2)).\
                      sort('ville').\
                      toDF('Ville','Jan','Fév','Mar',
                      'Avr','Mai','Jun','Jul','Aoû',
                      'Sep','Oct','Nov','Déc').\
                      cache()

#'Janvier','Février','Mars','Avril','Mai','Juin','Juillet','Août','Septembre','Octobre','Novembre','Décembre'

spark.sql('use cours_spark')
spark.sql('DROP TABLE IF EXISTS cours_spark.donneesMeteo12')
donneesMeteo12.write.saveAsTable('donneesMeteo12')
spark.sql('select * from cours_spark.donneesMeteo12 limit 10').show(10)
