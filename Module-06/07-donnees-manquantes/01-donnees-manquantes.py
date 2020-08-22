from pyspark.sql.functions import *

meteo.where('id < 8000').count()
description = meteo.where('id < 8000')\
     .select('annee','temperature', 'humidite',
             'visibilite', 'pression','precipitations')\
     .describe()\
     .select([c if c == 'summary'
                else round(c,2).alias(c)
                for c in
                    ['summary','annee','temperature', 'humidite',
                   'visibilite', 'pression','precipitations']])\
     .show()

meteo.where('id < 8000').where(meteo['temperature'].isNull()).count()
meteo.where('id < 8000').where(meteo['humidite'].isNull()).count()
meteo.where('id < 8000').where(meteo['visibilite'].isNull()).count()
meteo.where('id < 8000').where(meteo['pression'].isNull()).count()
meteo.where('id < 8000').where(meteo['precipitations'].isNull()).count()

meteo.where('id < 8000').toPandas().isna().sum()
meteoDataFrame.toPandas().isna().sum()

meteo.where('id < 8000')\
     .where(meteo['temperature'].isNotNull())\
     .where(meteo['humidite'].isNotNull() )\
     .where(meteo['visibilite'].isNotNull() )\
     .where(meteo['pression'].isNotNull() )\
     .count()

meteo.where('id < 8000')\
     .na.fill(0 ,["precipitations"])\
     .na.drop()\
     .count()

meteoNotNull =  meteo.where('id < 8000')\
                     .na.fill(0 ,["precipitations"])\
                     .na.drop()

meteo.where('id == 7020')\
     .where(meteo['temperature'].isNotNull())\
     .where(meteo['humidite'].isNotNull() )\
     .where(meteo['visibilite'].isNull() )\
     .where(meteo['pression'].isNull() )\
     .where(meteo['precipitations'].isNull())\
     .select('id','annee','mois_jour','temperature',
             'humidite','precipitations')\
     .show()

from databricks import koalas as ks
meteoDataFrame.to_koalas().isna().sum()



meteo = meteoDataFrame.select(
                 col('numer_sta'),
                 col('date')[0:4].cast('int') ,
                 col('date')[5:2].cast('int'),
                 col('date')[7:2].cast('int'),
                 col('date')[5:4],
                 round(col('t') - 273.15,2),
                 col('u') / 100 ,
                 col('vv') / 1000 ,
                 col('pres') / 1000,
                 col('rr1')*3,
                 col('rr3'),
                 col('rr6')/2,
                 col('rr12')/4,
                 col('rr24')/8)\
                   .toDF('id','annee','mois','jour','mois_jour','temperature',
                   'humidite','visibilite','pression',
                   'precipitations1','precipitations3','precipitations6',
                   'precipitations12','precipitations24')\
             .cache()

meteo.where('id < 8000')\
       .select('precipitations1','precipitations3','precipitations6',
             'precipitations12','precipitations24','temperature')\
       .toDF('prec1','prec3','prec6','prec12','prec24','temp')\
       .describe()\
       .select([c if c == 'summary'
                else round(c,2).alias(c)
                for c in['prec1','prec3','prec6',
                          'prec12','prec24','temp']])\
      .show()


meteo.select( coalesce('precipitations3','precipitations24',
                       'precipitations12','precipitations6',
                       'precipitations1').alias('precipitations')).show()


meteo.where('id < 8000')\
     .select( coalesce('precipitations24','precipitations12',
                       'precipitations6','precipitations3',
                       'precipitations1').alias('precipitationsH')
            ).describe().show()
