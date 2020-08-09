donnees00 = spark.sparkContext.\
       textFile('/user/spark/donnees/postesSynop.csv').\
       persist()
donnees00
donnees01 = donnees00.filter( lambda ligne :
                         str(ligne)[0].isdigit() )
donnees02 = donnees01.map( lambda ligne :
                          ligne.split(";"))
donnees00.id()
donnees01.id()
donnees02.id()
donnees02.take(5)




spark.conf.get('spark.driver.memory'),\
      spark.conf.get('spark.executor.cores'),\
      spark.conf.get('spark.executor.memory')

from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

def transformLigne(ligne):
   champs = ligne.split(";")
   return ( str(champs[0]),
            (int(str(champs[1])[0:4]),
            int(str(champs[1])[4:6]),
            int(str(champs[1])[6:8]),
            float(str(champs[7])) - 273.15,
            float(int(str(champs[9])) / 100 ),
            int(str(champs[10])),
            float(int(str(champs[20])) / 1000 )) )

donnees00 = spark.sparkContext. \
       textFile('/user/spark/donnees/meteo.txt')

donnees01 = donnees00.filter( lambda ligne :
                      str(ligne)[0:5].isdigit())

donnees02 = donnees01.map(lambda ligne:
                      str(ligne).replace('mq','0'))

donnees03 = donnees02.map(lambda ligne:
                      transformLigne(ligne)).persist()
donnees00.id()
donnees01.id()
donnees02.id()
donnees03.id()
donnees03.count()


donnees = spark.sparkContext. \
       textFile('/user/spark/donnees/meteo'). \
       filter( lambda ligne : str(ligne)[0:5].isdigit()). \
       map(lambda ligne: str(ligne).replace('mq','0')). \
       map(lambda ligne: transformLigne(ligne)). \
       persist()

donnees.count()
