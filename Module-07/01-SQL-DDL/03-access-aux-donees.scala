import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.
              config("spark.jars.packages",
                        "io.delta:delta-core_2.12:0.8.0").
              config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension").
              config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog").
              getOrCreate()

spark.sql("show databases").show()
spark.sql("use gest_comm")

spark.sql( "CREATE DATABASE deltaCours "+
          "LOCATION  \"hdfs:///user/spark/databases/deltaCours_db\"")
spark.sql("use deltaCours")

val fichiers = Array( "ACHETEURS","ADRESSES","AGENCES","CATEGORIES",
                    "CLIENTS","COMMANDES","COMMISSIONNEMENTS",
                    "COMMISSIONNEMENTS_AGENCES","COMMISSIONNEMENTS_VENDEURS",
                    "COORDONEES","DETAILS_COMMANDES","EMPLOYES","FACTURES",
                    "FOURNISSEURS","GESTIONS_STOCKS","MAGASINS",
                    "MOUVEMENTS","PRODUITS","RELANCES","STOCKS_ENTREPOTS",
                    "TVA_PRODUIT","VENDEURS","VILLES")

fichiers.foreach{fichier => spark.sql("CREATE TABLE "+fichier+
                              " USING DELTA LOCATION \"donnees/delta/"+
                              fichier+"_delta\"")}
                                      
