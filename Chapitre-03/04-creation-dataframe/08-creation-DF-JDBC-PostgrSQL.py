#pyspark \
#    --master spark://jupiter.olimp.fr:7077 \
#    --executor-cores 8 \
#    --executor-memory 20g \
#    --jars /usr/share/spark/jars/postgresql-42.2.14.jar

from pyspark.sql import SparkSession

spark = SparkSession.builder\
          .config('spark.jars.packages',
                         'postgresql:postgresql:42.2.14') \
          .config('spark.jars.packages',
                         'io.delta:delta-core_2.12:0.8.0') \
          .config('spark.sql.extensions',
                         'io.delta.sql.DeltaSparkSessionExtension')\
          .getOrCreate()

url        = 'jdbc:postgresql://192.168.1.25:5432/cours'
user       = 'stagiaire'
password   = 'CoursNFP107!'

format     = 'delta'
repertoire = 'donnees/delta/'
nom_table   = 'VILLES'

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
        .option('dbtable', 'produits') \
        .option('user', user) \
        .option('password', password)\
        .load()\
        .select('nom_produit','quantite','unites_stock')\
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
