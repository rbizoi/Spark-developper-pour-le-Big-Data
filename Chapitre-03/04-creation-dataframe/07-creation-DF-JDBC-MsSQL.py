from pyspark.sql import SparkSession

spark = SparkSession.builder\
          .config('spark.jars.packages',
                         'io.delta:delta-core_2.12:0.8.0') \
          .config('spark.sql.extensions',
                         'io.delta.sql.DeltaSparkSessionExtension')\
          .getOrCreate()

# ---------------------------------------------------------------------------------------------------------------------
servername = 'jdbc:sqlserver://192.168.1.25'
dbname     = 'cours'
url        = servername + ';' + 'databaseName=' + dbname + ';'
format     = 'com.microsoft.sqlserver.jdbc.spark'
# ---------------------------------------------------------------------------------------------------------------------
user       = 'sa'
password   = 'CoursNFP107!'
# ---------------------------------------------------------------------------------------------------------------------


donnees00 = spark.read \
        .format(format) \
        .option('url', url) \
        .option('dbtable', 'categories') \
        .option('user', user) \
        .option('password', password).load()
donnees00.show(3)

requette    = 'select titre,nom,prenom from employes'
donnees01 = spark.read \
        .format('jdbc') \
        .option('url', url) \
        .option('query', requette) \
        .option('user', user) \
        .option('password', password).load()
donnees01.show(3)
