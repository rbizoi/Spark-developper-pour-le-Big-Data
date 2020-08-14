# pyspark \
#    --master spark://jupiter.olimp.fr:7077 \
#    --executor-cores 8 \
#    --executor-memory 20g \
#    --packages mysql:mysql-connector-java:8.0.20
from pyspark.sql import SparkSession

spark = SparkSession.builder\
          .config('spark.jars.packages',
                         'mysql:mysql-connector-java:8.0.20') \
          .config('spark.jars.packages',
                         'io.delta:delta-core_2.12:0.7.0') \
          .config('spark.sql.extensions',
                         'io.delta.sql.DeltaSparkSessionExtension')\
          .getOrCreate()

base        = 'coursSPARK'
url         = 'jdbc:mysql://jupiter.olimp.fr:3306/'+ \
                   base+'?serverTimezone=UTC#'
user        = 'spark'
password    = 'CoursSPARK3#'
format     = 'delta'
repertoire = 'donnees/delta/'
fichiers   = ['ADRESSES','VILLES','FOURNISSEURS']

for nom_table in fichiers :
    donnees = spark.read.format(format).load(
                    repertoire+nom_table+'_'+format).cache()
    donnees.write.format('jdbc') \
            .option('url', url) \
            .option('dbtable', nom_table.lower()) \
            .option('user', user) \
            .option('password', password)\
            .save()

spark.read \
        .format('jdbc') \
        .option('url', url) \
        .option('dbtable', 'villes') \
        .option('user', user) \
        .option('password', password)\
        .load()\
        .select('VILLE','PROVINCE','PAYS')\
        .show(5)

requette = '''SELECT SOCIETE, VILLE
            FROM coursSPARK.fournisseurs fr
                 JOIN coursSPARK.adresses ad
                      ON ad.NO_ADRESSE = fr.NO_ADRESSE
                 JOIN coursSPARK.villes vl
                      ON ad.NO_VILLE = vl.NO_VILLE'''

donnees = spark.read \
        .format('jdbc') \
        .option('url', url) \
        .option('query', requette) \
        .option('user', user) \
        .option('password', password).load()

donnees.show(3)
