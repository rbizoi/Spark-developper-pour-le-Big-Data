from pyspark.sql import SparkSession
spark = SparkSession.builder.\
        appName("L'architecture application").\
        getOrCreate()

spark.conf.get('spark.driver.memory'),\
      spark.conf.get('spark.executor.cores'),\
      spark.conf.get('spark.executor.memory')


data = [('Ajaccio'     ,'dfa' ),
                  ('Angers'      ,'dfa' ),
                  ('Angoulème'   ,'dfa' ),
                  ('Besançon'    ,'dfa' ),
                  ('Biarritz'    ,'dfa' ),
                  ('Bordeaux'    ,'dfa' ),
                  ('Brest'       ,'dfa' ),
                  ('Caen'        ,'dfa' ),
                  ('Clermont-Fd' ,'dfa' ),
                  ('Dijon'       ,'dfa' ),
                  ('Embrun'      ,'dfa' ),
                  ('Grenoble'    ,'dfa' ),
                  ('Lille'       ,'dfa' ),
                  ('Limoges'     ,'dfa' ),
                  ('Lyon'        ,'dfa' ),
                  ('Marseille'   ,'dfa' ),
                  ('Montpellier' ,'dfa' ),
                  ('Nancy'       ,'dfa' ),
                  ('Nantes'      ,'dfa' ),
                  ('Nice'        ,'dfa' ),
                  ('Nîmes'       ,'dfa' ),
                  ('Orléans'     ,'dfa' ),
                  ('Paris'       ,'dfa' )]

dfa = spark.sparkContext.parallelize(data).toDF(['ville','valeur'])
rdd01 = spark.sparkContext.parallelize(data)
rdd02 = spark.sparkContext.parallelize(data,1)
rdd03 = spark.sparkContext.parallelize(data,3)

rdd01.partitions
rdd02.partitions
rdd03.partitions

data = [ ('Nancy'       ,'dfb' ),
          ('Nantes'      ,'dfb' ),
          ('Nice'        ,'dfb' ),
          ('Nîmes'       ,'dfb' ),
          ('Orléans'     ,'dfb' ),
          ('Paris'       ,'dfb' ),
          ('Perpignan'   ,'dfb' ),
          ('Poitiers'    ,'dfb' ),
          ('Reims'       ,'dfb' ),
          ('Rennes'      ,'dfb' ),
          ('Rouen'       ,'dfb' ),
          ('St-Quentin'  ,'dfb' ),
          ('Strasbourg'  ,'dfb' ),
          ('Toulon'      ,'dfb' ),
          ('Toulouse'    ,'dfb' ),
          ('Tours'       ,'dfb' ),
          ('Vichy'       ,'dfb' )]

dfb = spark.sparkContext.parallelize(data).toDF(['ville','valeur'])


rdd04 = spark.sparkContext.\
       textFile('/user/spark/donnees/postesSynop.csv').\
       persist()

rdd04 = spark.sparkContext.\
       textFile('/user/spark/donnees/postesSynop.csv',8).\
       persist()

rdd04.partitions
rdd05.partitions
