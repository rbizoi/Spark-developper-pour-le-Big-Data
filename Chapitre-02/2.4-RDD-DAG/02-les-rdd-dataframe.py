spark.conf.get('spark.driver.memory')
spark.conf.get('spark.executor.cores')
spark.conf.get('spark.executor.memory')

from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

def transformLigne(ligne):
   champs = ligne.split(";")
   return  (    str(champs[0]),
                int(str(champs[1])[0:4]),
                int(str(champs[1])[4:6]),
                int(str(champs[1])[6:8]),
                float(str(champs[7])) - 273.15,
                float(int(str(champs[9])) / 100 ),
                int(str(champs[10])),
                float(int(str(champs[20])) / 1000 )   )

donnees = spark.sparkContext. \
       textFile('/user/spark/donnees/meteo'). \
       filter( lambda ligne : str(ligne)[0:5].isdigit()). \
       map(lambda ligne: str(ligne).replace('mq','0')). \
       map(lambda ligne: transformLigne(ligne)). \
       persist()

donnees.count()

from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

def transformLigne(ligne):
   champs = ligne.split(";")
   return  (    str(champs[0]),
                int(str(champs[1])[0:4]),
                int(str(champs[1])[4:6]),
                int(str(champs[1])[6:8]),
                float(str(champs[7])) - 273.15,
                float(int(str(champs[9])) / 100 ),
                int(str(champs[10])),
                float(int(str(champs[20])) / 1000 )   )

donnees = spark.sparkContext. \
       textFile('/user/spark/donnees/meteo.txt'). \
       filter( lambda ligne : str(ligne)[0:5].isdigit()). \
       map(lambda ligne: str(ligne).replace('mq','0')). \
       map(lambda ligne: transformLigne(ligne)). \
       persist()

donnees.count()

donnees.take(2)
schema = StructType([
            StructField('station'     , StringType() , True),
            StructField('annee'       , IntegerType(), True),
            StructField('mois'        , IntegerType(), True),
            StructField('jour'        , IntegerType(), True),
            StructField('temperature' , FloatType()  , True),
            StructField('humidite'    , FloatType(), True),
            StructField('visibilite'  , IntegerType(), True),
            StructField('pression'    , FloatType()  , True)])

donneesStations = spark.createDataFrame(donnees, schema)
donneesStations.show(3)
exit()
