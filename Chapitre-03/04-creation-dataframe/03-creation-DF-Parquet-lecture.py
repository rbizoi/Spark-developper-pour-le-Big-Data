
listeRelTab = spark.read.parquet('ListeRelationsTables.parquet').toPandas()
listeTables = spark.read.parquet('ListeTables.parquet').toPandas()
listeColTables = spark.read.parquet('ListeTablesColonnees.parquet').toPandas()


hdfs dfs -ls donnees/etl/stagiaire/Oracle/EMPLOYES.parque

drwxr-xr-x   - spark hdfs          0 2020-08-12 16:09 donnees/etl/dictionnaireMetadonnees/Oracle/ListeRelationsTables.parquet
drwxr-xr-x   - spark hdfs          0 2020-08-12 16:09 donnees/etl/dictionnaireMetadonnees/Oracle/ListeTables.parquet
drwxr-xr-x   - spark hdfs          0 2020-08-12 16:09 donnees/etl/dictionnaireMetadonnees/Oracle/ListeTablesColonnees.parquet

repertoire = '/'
fichier = 'donnees/etl/stagiaire/Oracle/EMPLOYES.parquet'
format  = 'parquet'



donnees.write.mode("overwrite").format(format).save('donnees/etl/stagiaire/Oracle/EMPLOYES.delta')
donnees01 = spark.read.format(format).load(fichier)



donnees = spark.read.parquet(fichier)
donnees.select("titre","nom","prenom",).show(10,False)


donnees01 = spark.sql("select * from parquet."+
                      "`donnees/etl/stagiaire/Oracle/EMPLOYES.parquet`  "+
                      "where TITRE = 'M.'")

fichier = 'donnees/etl/stagiaire/Oracle/COMMANDES.parquet'
donnees = spark.read.parquet(fichier)
donnees.show(10,False)
