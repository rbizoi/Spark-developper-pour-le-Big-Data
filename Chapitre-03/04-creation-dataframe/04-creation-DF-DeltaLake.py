pyspark \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 8 \
    --executor-memory 20g \
    --packages io.delta:delta-core_2.12:0.7.0

fichier = 'donnees/etl/stagiaire/Oracle/EMPLOYES.parquet'
donnees = spark.read.parquet(fichier)
donnees.select("titre","nom","prenom",).show(10,False)

donnees.write.mode("overwrite").format('delta').save('donnees/etl/stagiaire/Oracle/EMPLOYES.delta')
donnees01 = spark.read.format('delta').load('donnees/etl/stagiaire/Oracle/EMPLOYES.delta')



url  = "jdbc:mysql://jupiter.olimp.fr:3306/cours?serverTimezone=UTC#"
user        = "spark"
password    = "CoursSPARK3#"


formatDictionnaire     = "parquet"



donnees00 = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", 'categories') \
        .option("user", user) \
        .option("password", password).load()
donnees00.show(3)

requette    = "select titre,nom,prenom from employes"
donnees01 = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("query", requette) \
        .option("user", user) \
        .option("password", password).load()
donnees01.show(3)
