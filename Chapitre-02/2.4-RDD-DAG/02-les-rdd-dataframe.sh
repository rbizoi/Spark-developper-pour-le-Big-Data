
from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

def transformLigne(ligne):
   champs = ligne.split(";")
   return  (    str(champs[0]),
                 str(champs[1]),
                 float(champs[2]),
                 float(champs[3]),
                 int(champs[4]) )


donnees = spark.sparkContext.\
       textFile('/user/spark/donnees/postesSynop.csv').\
       persist()
donnees = donnees.filter( lambda ligne :
                          str(ligne)[0].isdigit()).\
       map(lambda ligne: transformLigne(ligne) )

schema = StructType([
            StructField('station'  , StringType(), True),
            StructField('nom'      , StringType(), True),
            StructField('latitude' , FloatType(), True),
            StructField('longitude', FloatType(), True),
            StructField('altitude' , IntegerType(), True)])


donneesStations = spark.createDataFrame(donnees, schema)
donneesStations.show()
exit()
