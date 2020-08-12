
mysql --user=root

CREATE DATABASE cours;
GRANT ALL PRIVILEGES ON cours.* TO 'spark'@'localhost';
GRANT ALL PRIVILEGES ON cours.* TO 'spark'@'%';
FLUSH PRIVILEGES;
USE cours;





mysql --user=spark --password='CoursSPARK3#' --database=cours


'donnees/etl/dictionnaireMetadonnees/Oracle/ListeRelationsTables.parquet'
'donnees/etl/dictionnaireMetadonnees/Oracle/ListeTables.parquet'

export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_DRIVER_PYTHON_OPTS=''

pyspark \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 8 \
    --executor-memory 20g \
    --packages mysql:mysql-connector-java:8.0.20

listeRelTab = spark.read.parquet('donnees/etl/dictionnaireMetadonnees/Oracle/ListeRelationsTables.parquet').toPandas()
listeTables = spark.read.parquet('donnees/etl/dictionnaireMetadonnees/Oracle/ListeTables.parquet').toPandas()
listeColTables = spark.read.parquet('donnees/etl/dictionnaireMetadonnees/Oracle/ListeTablesColonnees.parquet').toPandas()


# ---------------------------------------------------------------------------------------------------------------------
url  = "jdbc:mysql://jupiter.olimp.fr:3306/cours?serverTimezone=UTC#"
# ---------------------------------------------------------------------------------------------------------------------
user        = "spark"
password    = "CoursSPARK3#"
# ---------------------------------------------------------------------------------------------------------------------
requette    = "select * from categories"

for nom_table in listeTables.nom_table :
    donnees = spark.read.parquet(
                'donnees/etl/stagiaire/Oracle/'+
                 nom_table+'.parquet')
    donnees.write.format('jdbc') \
            .option('url', servername) \
            .option('dbtable', nom_table.lower()) \
            .option('user', user) \
            .option('password', password)\
            .save()

listeValeurs = spark.read \
        .format("jdbc") \
        .option("url", servername) \
            .option("query", requette) \
            .option("user", user) \
            .option("password", password).load()
listeValeurs.show()

listeValeurs = spark.read \
        .format("jdbc") \
        .option("url", servername) \
        .option("dbtable", 'categories') \
        .option("user", user) \
        .option("password", password).load()
listeValeurs.show()

for nom_table in ['MAGASINS','MOUVEMENTS',
                'PRODUITS','RELANCES',
                'STOCKS_ENTREPOTS','TVA_PRODUIT',
                'VENDEURS','VILLES','FOURNISSEURS']:
    donnees = spark.read.parquet(
                'donnees/etl/stagiaire/Oracle/'+
                 nom_table+'.parquet')
    donnees.write.format('jdbc') \
            .option('url', servername) \
            .option('dbtable', nom_table.lower()) \
            .option('user', user) \
            .option('password', password)\
            .save()


            
