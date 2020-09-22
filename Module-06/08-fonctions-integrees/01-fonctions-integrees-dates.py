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


meteo.select('date',
              date_add('date',1).alias('demain'),
              date_add('date',-1).alias('hier'),
              add_months('date',1).alias('mois dernier'),
              add_months('date',-1).alias('mois prochain'),
              )\
     .show(1)


meteo.select('date',
              next_day  ('date',"Wed").alias("prochain mercredi"),
              last_day  ('date').alias("dernier jour du mois"),
            ).show(1)



meteo.select('date',
              current_date().alias("date du jour"),
              datediff(current_date(),'date').alias("jours"),
              months_between(current_date(),'date').alias("mois"),
              months_between(current_date(),
                             date_add('date',19)).alias("mois"),
            ).show(1)


meteo.orderBy(desc('timestamp'))\
      .select('timestamp',
              date_trunc ('year','date').cast('date').alias('y'),
              date_trunc ('month','date').cast('date').alias('m'),
              dayofyear ('timestamp').alias('dy'),
              dayofmonth('timestamp').alias('dm'),
              dayofweek ('timestamp').alias('dw'),
              quarter   ('timestamp').alias('q'),
              month     ('timestamp').alias('m'),
              hour      ('timestamp').alias('h'),
            ).show(1)


meteoFance = meteo.where('id < 8000')\
             .join(villes.withColumnRenamed('Id', 'id'),'id')\
             .select(initcap(regexp_replace('ville','-',' ')).alias('ville'),
                     'date','annee','mois','jour','temperature',
                     'humidite','visibilite','pression','precipitations')


from pyspark.sql.functions import window
meteoFance.where("ville = 'Mont De Marsan' and \
                 annee = 2019")\
           .groupBy(window('date', '5 week'))\
           .agg( round(avg('temperature'),3).alias('temperature'  ),
                round(sum('precipitations'),3).alias('precipitations'))\
           .orderBy('window')\
           .show(15,truncate=False)

meteoFance.count()
meteoFance.selectExpr('ville','date','temperature as t',
                      'humidite as h','visibilite as v',
                      'pression as p','precipitations as e').show()

meteoFance.where("ville = 'Mont De Marsan' and \
                 annee = 2019")\
           .groupBy(window('date', '5 week'))\
           .agg( round(avg('temperature'),3).alias('temperature'  ),
                round(sum('precipitations'),3).alias('precipitations'))\
           .orderBy('window')\
           .show(15,truncate=False)

meteoFance.where("ville = 'Mont De Marsan' and annee = 2019")\
          .groupBy(window('date', '24 day'))\
          .agg( round(avg('temperature'),3).alias('temperature'  ),
               round(sum('precipitations'),3).alias('precipitations'))\
          .orderBy('window')\
          .show(15,truncate=False)            
