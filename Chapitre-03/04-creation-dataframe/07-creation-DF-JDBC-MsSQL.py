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
            .option('query', requette) \
            .option('user', user) \
            .option('password', password)\
            .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
            .load()\
            .show()
