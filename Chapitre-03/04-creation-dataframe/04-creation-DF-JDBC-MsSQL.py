pyspark \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 8 \
    --executor-memory 20g \
    --packages mysql:mysql-connector-java:8.0.20

url  = "jdbc:mysql://jupiter.olimp.fr:3306/cours?serverTimezone=UTC#"
user        = "spark"
password    = "CoursSPARK3#"

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
