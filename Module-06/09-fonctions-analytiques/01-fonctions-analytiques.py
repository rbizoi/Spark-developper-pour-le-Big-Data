from pyspark.sql.functions import *

meteoFance = meteo.where('id < 8000')\
             .join(villes.withColumnRenamed('Id', 'id'),'id')\
             .select(initcap(regexp_replace('ville','-',' ')).alias('ville'),
                     'annee','mois','jour','temperature',
                     'humidite','visibilite','pression','precipitations')

meteoFance.count()
meteoFance.selectExpr('ville','annee','mois','jour','temperature as t',
                      'humidite as h','visibilite as v',
                      'pression as p','precipitations as e').show()


meteoFance.where("ville = 'Strasbourg Entzheim' and annee = 2019")\
           .selectExpr('ville','annee','mois','temperature as t',
                      'humidite as h','visibilite as v',
                      'pression as p','precipitations as e').show()
