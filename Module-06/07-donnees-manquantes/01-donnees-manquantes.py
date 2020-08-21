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
