clear
export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_DRIVER_PYTHON_OPTS=''
pyspark \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 1 \
    --executor-memory 2g \
    --conf spark.executor.instances=1 \
    --conf spark.deploy.defaultCores=1 \
    --conf spark.default.parallelism=1

pyspark --master spark://jupiter.olimp.fr:7077


print( 'spark.driver.memory = %s'\
       %spark.conf.get('spark.driver.memory'))
print( 'spark.executor.cores = %s'\
       %spark.conf.get('spark.executor.cores'))
print( 'spark.executor.memory = %s'\
       %spark.conf.get('spark.executor.memory'))
spark.conf.get('spark.driver.memory')
spark.conf.get('spark.executor.cores')
spark.conf.get('spark.executor.memory')


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,length,lower,regexp_replace

spark = SparkSession.builder.\
        appName("Lecture  du flux des tweets").getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', 8)






spark.conf.set('spark.default.parallelism', 1)
spark.conf.set('spark.deploy.defaultCores', 1)

spark.conf.set('spark.executor.instances',1)


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
donnees.take(2)


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






donnees.saveAsTextFile('/user/spark/donnees/meteo_traite')

hadoop fs -cat /user/spark/donnees/meteo_traite/* | hadoop fs -put - /user/spark/donnees/meteo.txt




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




from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


def printConfigs(session) = {
mconf = session.conf.getAll()
for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}") }
}

getAll




.keySet


import org.apache.spark.sql.SparkSession

object donneesMeteo {

  case class MeteoFiltre(station:String,
                   annee:String,
                   mois:String,
                   jour:String,
                   temperature:String,
                   pression:String)

  def parseLineMeteoFiltre(line: String): MeteoFiltre = {
    val fields = line.split(';')

    val meteo: MeteoFiltre = MeteoFiltre(
      fields(0),
      fields(1).substring(0,4),
      fields(1).substring(4,6),
      fields(1).substring(6,8),
      fields(7),
      fields(20))

    return meteo
  }

case class Meteo(station:String,
                   annee:Int,
                   mois:Int,
                   jour:Int,
                   temperature:Double,
                   pression:Int)

  def parseLineMeteo(line: MeteoFiltre): Meteo = {
    val meteo: Meteo = Meteo(line.station,
                             line.annee.toInt,
                             line.mois.toInt,
                             line.jour.toInt,
                             line.temperature.toDouble,
                             line.pression.toInt)

      return meteo
}


val donneesMeteo = spark.sparkContext.textFile("donnees/meteo")
                   .filter( l => l.substring(0,4).matches("^\\d\\d\\d\\d$"))
                   .map(parseLineMeteoFiltre)
                   .filter(l => l.temperature.matches("[\\d.]+"))
                   .filter(l => l.pression.matches("\\d+"))
                   .map(parseLineMeteo)

    println("-"*50)
    donneesMeteo.take(3).foreach(println)
    println("-"*50)

    val donneesMeteoDF = spark.createDataFrame(donneesMeteo)
    donneesMeteoDF.printSchema()
    //donneesMeteoDF.write.parquet("C:\\SolutionsSpark\\donnees\\meteo30_parquet")
    val parquetFileDF = spark.read.parquet("C:\\SolutionsSpark\\donnees\\meteo30_parquet")
    parquetFileDF.createOrReplaceTempView("donneesMeteo")

  }
}




val conf = new SparkConf()
  .setMaster(...)
  .setAppName(...)
  .set("spark.cores.max", "10")
val sc = new SparkContext(conf)
