
stop-all.sh


cat <<FIN_FICHIER > $SPARK_HOME/conf/spark-defaults.conf
spark.serializer                     org.apache.spark.serializer.KryoSerializer
spark.io.compression.lz4.blockSize   128kb
#--------------------------------------------------------------------------------
spark.yarn.jars                      hdfs:///spark-jars
#--------------------------------------------------------------------------------
spark.master                         yarn
#--------------------------------------------------------------------------------
spark.eventLog.dir                   hdfs:///spark-history/
spark.eventLog.enabled               true
#--------------------------------------------------------------------------------
spark.history.fs.cleaner.enabled     true
spark.history.fs.cleaner.interval    7d
spark.history.fs.cleaner.maxAge      90d
spark.history.fs.logDirectory        hdfs:///spark-history/
#--------------------------------------------------------------------------------
FIN_FICHIER


cat <<FIN_FICHIER > $SPARK_HOME/conf/spark-defaults.conf
spark.serializer                     org.apache.spark.serializer.KryoSerializer
spark.io.compression.lz4.blockSize   128kb
#--------------------------------------------------------------------------------
spark.yarn.jars                      hdfs:///spark-jars
#--------------------------------------------------------------------------------
spark.master                         yarn
#spark.driver.memory                  1g
#spark.executor.memory                1g
#spark.executor.cores                  1
#spark.executor.instances             2
#spark.default.parallelism            8
#--------------------------------------------------------------------------------
spark.eventLog.dir                   hdfs:///spark-history/
spark.eventLog.enabled               true
#--------------------------------------------------------------------------------
spark.history.fs.cleaner.enabled     true
spark.history.fs.cleaner.interval    7d
spark.history.fs.cleaner.maxAge      90d
spark.history.fs.logDirectory        hdfs:///spark-history/
#--------------------------------------------------------------------------------
#spark.deploy.defaultCores            8
FIN_FICHIER

scp -r /usr/share/spark/conf/spark-defaults.conf        vulcain:/usr/share/spark/conf/spark-defaults.conf
scp -r /usr/share/spark/conf/spark-defaults.conf        junon:/usr/share/spark/conf/spark-defaults.conf
scp -r /usr/share/spark/conf/spark-defaults.conf        pluton:/usr/share/spark/conf/spark-defaults.conf

start-all.sh

spark-shell --master spark://jupiter.olimp.fr:7077


export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_DRIVER_PYTHON_OPTS=''


spark-shell \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 1 \
    --executor-memory 1g


import org.apache.spark.sql.SparkSession

def printConfigs(session: SparkSession) = {
  val mconf = session.conf.getAll
  for (k <- mconf.keySet) {
      if (k.matches("spark.executor.*"))
           println(s"${k} -> ${mconf(k)}")
    }
}

printConfigs(spark)




spark.conf.set("spark.executor.cores",1)






var lines = spark.sparkContext.textFile("donnees/livres")
val transfLines = lines.map(x => x.toLowerCase).
         map(x => x.replaceAll("[^\\p{L}]", " "))
val mots = transfLines.flatMap(x => x.split(" "))
val motsAvec1 = mots.map(t => (t, 1))
val motsOccurences = motsAvec1.reduceByKey((a, b) => a + b)
val motsFiltres = motsOccurences.filter(w => w._1.length() > 3)
val occurecesMots = motsFiltres.map( x => (x._2,x._1))
val occurecesOrdonnees = occurecesMots.sortByKey(ascending = false)

val sortie = occurecesOrdonnees.collect()
