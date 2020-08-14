from pyspark.sql import SparkSession

spark = SparkSession.builder\
          .config('spark.jars.packages',
                         'sqlserver:spark-mssql-connector_2.12:1.0.0') \
          .config('spark.jars.packages',
                         'io.delta:delta-core_2.12:0.8.0') \
          .config('spark.sql.extensions',
                         'io.delta.sql.DeltaSparkSessionExtension')\
          .getOrCreate()
# ---------------------------------------------------------------------------------------------------------------------
servername = 'jdbc:sqlserver://192.168.1.25'
dbname     = 'cours'
url        = servername + ';' + 'databaseName=' + dbname + ';'
format     = 'com.microsoft.sqlserver.jdbc.spark'
# ---------------------------------------------------------------------------------------------------------------------
user       = 'sa'
password   = 'CoursNFP107!'
# ---------------------------------------------------------------------------------------------------------------------


donnees00 = spark.read \
        .format('com.microsoft.sqlserver.jdbc.spark') \
        .option('url', url) \
        .option('dbtable', 'categories') \
        .option('user', user) \
        .option('password', password).load()
donnees00.show(3)


# ---------------------------------------------------------------------------------------------------------------------
servername             = "jdbc:sqlserver://192.168.1.25"
dbname                 = "cours"
urlMSQL                = servername + ";" + "databaseName=" + dbname + ";"
# ---------------------------------------------------------------------------------------------------------------------
userMSQL               = "sa"
passwordMSQL           = "CoursNFP107!"
# ---------------------------------------------------------------------------------------------------------------------
formatMSSQLJDBC        = "com.microsoft.sqlserver.jdbc.spark"
# ---------------------------------------------------------------------------------------------------------------------
requette = 'select * from stagiaire.categories'


def lectureSQLDataFrame(requette, format=formatMSSQLJDBC, url=urlMSQL, user=userMSQL, password=passwordMSQL):
    """
       la lecture à partir de la base de données de la requette et alimentation du DataFrame en retour
    """
    #            .format(connector_type) \
    listeValeurs = spark.read \
        .format(format) \
        .option("url", url) \
            .option("query", requette) \
            .option("user", user) \
            .option("password", password).load()
    return listeValeurs


requette = """
            select sc.name as nom_schema,
                   ob.name as nom_table
            from sys.all_objects ob
                 join sys.schemas sc
                    on ob.schema_id = sc.schema_id
            where ob.type = 'U'
              and sc.schema_id = 5
            """























































#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ---------------------------------------------------------------------------------------------------------------------
servername             = "jdbc:sqlserver://192.168.1.25"
dbname                 = "cours"
urlMSQL                = servername + ";" + "databaseName=" + dbname + ";"
# ---------------------------------------------------------------------------------------------------------------------
userMSQL               = "sa"
passwordMSQL           = "CoursNFP107!"
# ---------------------------------------------------------------------------------------------------------------------
repertoireDictionnaire = "dictionnaireMetadonnees/SQLServer"
formatMSSQLJDBC        = "com.microsoft.sqlserver.jdbc.spark"
formatDictionnaire     = "parquet"
# ---------------------------------------------------------------------------------------------------------------------

def lectureSQLDataFrame(requette, format=formatMSSQLJDBC, url=urlMSQL, user=userMSQL, password=passwordMSQL):
    """
       la lecture à partir de la base de données de la requette et alimentation du DataFrame en retour
    """
    #            .format(connector_type) \
    listeValeurs = spark.read \
        .format(format) \
        .option("url", url) \
            .option("query", requette) \
            .option("user", user) \
            .option("password", password).load()
    return listeValeurs


lectureSQLDataFrame('select * from categories').show()
