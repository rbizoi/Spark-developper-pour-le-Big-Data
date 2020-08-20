from pyspark.sql.functions import *

meteoDataFrame.columns

df01 = meteoDataFrame.select('date')\
         .withColumn('d00',col('date')[0:4])\
         .withColumn('d01',to_date(col('date')[0:8],'yyyyMMdd'))\
         .withColumn('d02',to_timestamp(col('date').cast('string'),'yyyyMMddHHmmss'))
df01.show(10)
df01.printSchema()

unix_timestamp



meteo = meteoDataFrame.select(
         col('numer_sta'),
         to_date(col('date')[0:8],'yyyyMMdd'),
         to_timestamp(col('date').cast('string'),'yyyyMMddHHmmss'),
         col('date')[0:4].cast('int') ,
         col('date')[5:2].cast('int'),
         col('date')[7:2].cast('int'),
         col('date')[5:4],
         round(col('t') - 273.15,2),
         col('u') / 100 ,
         col('vv') / 1000 ,
         col('pres') / 1000,
         col('rr1'))\
     .toDF('id','date','timestamp','annee','mois','jour','mois_jour',
           'temperature','humidite','visibilite','pression','precipitations')\
     .cache()

meteo.select('date','timestamp','temperature','humidite',
             'visibilite','pression').show(3)

meteo.select(date_format('date','d dd DDD M MMM MMMM yy yyyy')).show(3,truncate=False)

meteo.sample(0.3)\
     .orderBy(desc('timestamp'))\
     .select(date_format('timestamp','h H m s S'))\
     .show(10,truncate=False)

meteo.sample(0.3)\
     .orderBy(desc('timestamp'))\
     .select(date_format('date','q Q E F'))\
     .show(3,truncate=False)


                      
