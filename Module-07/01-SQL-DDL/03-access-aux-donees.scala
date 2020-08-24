import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.
              config("spark.jars.packages",
                        "io.delta:delta-core_2.12:0.8.0").
              config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension").
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

spark.sql("SELECT CL.SOCIETE                           "+
"        , EXTRACT ( YEAR FROM DATE_COMMANDE) ANNEE    "+
"        , EXTRACT ( MONTH FROM DATE_COMMANDE) MOIS    "+
"        , COUNT(DISTINCT CO.NO_COMMANDE) NB_COMMANDES "+
"        , SUM(DC.PORT) PORT                           "+
"        , SUM(DC.QUANTITE) QUANTITE                   "+
"FROM CLIENTS CL                                       "+
"    JOIN MAGASINS  MA                                 "+
"             ON MA.CODE_CLIENT = CL.CODE_CLIENT       "+
"    JOIN ACHETEURS AC                                 "+
"             ON AC.NO_MAGASIN = MA.NO_MAGASIN         "+
"    JOIN COMMANDES CO                                 "+
"             ON CO.NO_ACHETEUR = AC.NO_ACHETEUR       "+
"    JOIN DETAILS_COMMANDES DC                         "+
"             ON DC.NO_COMMANDE = CO.NO_COMMANDE       "+
"GROUP BY CL.SOCIETE                                   "+
"        , EXTRACT ( YEAR FROM DATE_COMMANDE)          "+
"        , EXTRACT ( MONTH FROM DATE_COMMANDE)         "+
"ORDER BY CL.SOCIETE                                   "+
"        , EXTRACT ( YEAR FROM DATE_COMMANDE)          "+
"        , EXTRACT ( MONTH FROM DATE_COMMANDE)         ").show()
