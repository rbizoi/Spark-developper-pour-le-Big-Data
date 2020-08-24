export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_DRIVER_PYTHON_OPTS=''

# ensemble
pyspark \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 4 \
    --executor-memory 10g

spark-shell \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 4 \
    --executor-memory 10g


# seul

pyspark \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 8 \
    --executor-memory 20g \
    --jars

spark-shell \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 8 \
    --executor-memory 20g


    spark.conf.get('spark.driver.memory'),\
          spark.conf.get('spark.executor.cores'),\
          spark.conf.get('spark.executor.memory')

spark-sql \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 8 \
    --executor-memory 20g



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
