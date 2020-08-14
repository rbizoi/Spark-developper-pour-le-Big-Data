
from pyspark.sql import SparkSession

val spark = SparkSession.builder\
          .config("spark.jars.packages",
                         "io.delta:delta-core_2.12:0.7.0") \
          .config("spark.sql.extensions",
                         "io.delta.sql.DeltaSparkSessionExtension")\
          .getOrCreate()

val url = "jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS =(PROTOCOL=TCP)(HOST=192.168.1.25)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=pdbcours1)))"
val user = "stagiaire"
val password = "CoursSPARK3#"

val donnees00 = spark.read.
                      format("jdbc").
                      option("url", url).
                      option("dbtable", "categories").
                      option("user", user).
                      option("password", password).
                      load()

val donnees00.show(3)

val requette    = "select titre,nom,prenom from employes"
val donnees01 = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("query", requette) \
        .option("user", user) \
        .option("password", password).load()
val donnees01.show(3)
