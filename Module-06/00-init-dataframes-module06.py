from pyspark.sql.functions import *
from pyspark.sql.types     import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

spark = SparkSession.builder\
          .config("spark.jars.packages",
                         "io.delta:delta-core_2.12:0.8.0") \
          .config("spark.sql.extensions",
                         "io.delta.sql.DeltaSparkSessionExtension")\
          .getOrCreate()

meteoDataFrame  = spark.read.format('csv')\
    .option('sep',';')\
    .option('header','true')\
    .option('nullValue','mq')\
    .option('inferSchema', 'true')\
    .load('donnees/meteo')\
    .cache()

meteoDataFrame.columns
meteoDataFrame.printSchema()

schema = StructType([
        StructField('Id'           , StringType() , True),
        StructField('ville'        , StringType() , True),
        StructField('latitude'     , FloatType() , True),
        StructField('longitude'    , FloatType() , True),
        StructField('altitude'     , IntegerType() , True)])

villes  = spark.read.format('csv')   \
      .option('sep',';')                \
      .option('mergeSchema', 'true')    \
      .option('header','true')          \
      .schema(schema)                   \
      .load('/user/spark/donnees/postesSynop.csv')  \
      .cache()

@udf("string")
def formatVille(ville):
    if ville in ['CLERMONT-FD','MONT-DE-MARSAN',
                                   'ST-PIERRE','ST-BARTHELEMY METEO'] :
        return ville.title()
    else :
        if ville.find('-') != -1 :
            return ville[0:ville.find('-')].title()
        else:
            return ville.title()

villesT  = villes.select(
                col('Id').alias('id'),
                formatVille('ville').alias('ville'),
               'latitude',
               'longitude',
               'altitude')


meteo = meteoDataFrame.select(
                 col('numer_sta'),
                 to_timestamp(col('date').cast('string'),'yyyyMMddHHmmss'),
                 col('date')[0:4].cast('int') ,
                 col('date')[5:2].cast('int'),
                 col('date')[7:2].cast('int'),
                 col('date')[5:4],
                 round(col('t') - 273.15,2),
                 col('u') / 100 ,
                 col('vv') / 1000 ,
                 col('pres') / 1000,
                 coalesce( col('rr3'),
                           col('rr24')/8,
                           col('rr12')/4,
                           col('rr6')/2,
                           col('rr1')*3  ) )\
             .toDF('id','date','annee','mois','jour','mois_jour','temperature',
                   'humidite','visibilite','pression','precipitations')\
             .cache()

meteo.select('annee','mois','jour','temperature','humidite',
             'visibilite','pression').show(3)

meteoFance = meteo.where('id < 8000')\
             .join(villesT,'id')\
             .select(initcap(regexp_replace('ville','-',' ')).alias('ville'),
                     'annee','mois','jour','temperature',
                     'humidite','visibilite','pression','precipitations')

meteoFance.write\
       .mode('overwrite')\
       .format('parquet')\
       .partitionBy('annee')\
       .option('path', '/user/spark/donnees/meteoFrance')\
       .save()

meteo.join(villesT,'id')\
     .select(initcap(regexp_replace('ville','-',' ')).alias('ville'),
                     'annee','mois','jour','temperature',
                     'humidite','visibilite','pression','precipitations')\
     .write\
     .mode('overwrite')\
     .format('parquet')\
     .partitionBy('annee')\
     .option('path', '/user/spark/donnees/meteoGlobal')\
     .save()

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
