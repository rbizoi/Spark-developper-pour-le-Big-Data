from pyspark.sql import SparkSession

spark = SparkSession.builder\
          .config("spark.jars.packages",
                         "io.delta:delta-core_2.12:0.8.0") \
          .config("spark.sql.extensions",
                         "io.delta.sql.DeltaSparkSessionExtension")\
          .getOrCreate()

repertoire = 'donnees/parquet/'
format     = '.parquet'
fichiers   = ['ACHETEURS','ADRESSES','AGENCES','CATEGORIES',
    'CLIENTS','COMMANDES','COMMISSIONNEMENTS',
    'COMMISSIONNEMENTS_AGENCES','COMMISSIONNEMENTS_VENDEURS',
    'COORDONEES','DETAILS_COMMANDES','EMPLOYES','FACTURES',
    'FOURNISSEURS','GESTIONS_STOCKS','MAGASINS',
    'MOUVEMENTS','PRODUITS','RELANCES','STOCKS_ENTREPOTS',
    'TVA_PRODUIT','VENDEURS','VILLES']

for nom in fichiers :
    donnees = spark.read.format('parquet'
                         ).load(repertoire+nom+'_parquet').cache()
    donnees.write.mode('overwrite').format('delta'
                         ).save('donnees/delta/'+nom+'_delta')
