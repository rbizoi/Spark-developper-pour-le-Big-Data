pyspark \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 8 \
    --executor-memory 20g \
    --jars /usr/share/spark/jars/mysql-connector-java-8.0.20.jar

from pyspark.sql import SparkSession

spark = SparkSession.builder\
          .config('spark.jars.packages',
                         'io.delta:delta-core_2.12:0.8.0') \
          .config('spark.sql.extensions',
                         'io.delta.sql.DeltaSparkSessionExtension')\
          .getOrCreate()

servername = 'jdbc:sqlserver://192.168.1.25'
dbname     = 'cours'
url        = servername + ';' + 'databaseName=' + dbname + ';'
user       = 'sa'
password   = 'CoursNFP107!'

listeValeurs = spark.read \
            .format('jdbc') \
            .option('url', url) \
            .option('dbtable', 'categories')\
            .option('user', user) \
            .option('password', password)\
            .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
            .load()\
            .show(3)

format = "com.microsoft.sqlserver.jdbc.spark"
requette   = 'select titre, nom, prenom from employes'
listeValeurs = spark.read \
            .format(format) \
            .option('url', url) \
            .option('query', requette) \
            .option('user', user) \
            .option('password', password)\
            .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
            .load()\
            .show(3)




        delta-core_2.12-0.8.0-SNAPSHOT.jar
        derby-10.12.1.1.jar
        mysql-connector-java-8.0.20.jar
        spark-mssql-connector_2.12-1.0.0.jar
        put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
