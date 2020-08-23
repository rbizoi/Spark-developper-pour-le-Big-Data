from pyspark.sql.functions import *
from pyspark.sql import Window

meteoFance = meteo.where('id < 8000')\
             .join(villes.withColumnRenamed('Id', 'id'),'id')\
             .select(initcap(regexp_replace('ville','-',' ')).alias('ville'),
                     'annee','mois','jour','temperature',
                     'humidite','visibilite','pression','precipitations')

meteoFance.count()
meteoFance.selectExpr('ville','annee','mois','jour','temperature as t',
                      'humidite as h','visibilite as v',
                      'pression as p','precipitations as e').show()

meteoMM = meteoFance.where("ville = 'Mont De Marsan' and \
                                annee = 2019")\
                     .select('mois','jour','temperature','humidite','visibilite',
                             'pression','precipitations')
meteoMM.show()


jourPOby = Window.partitionBy('mois').orderBy('jour')
jourOby  = Window.orderBy('mois','jour')
meteoMM.where("annee = 2019")\
       .groupBy('mois','jour')\
       .agg( round(sum('precipitations'),2).alias('prec'))\
       .select('mois','jour',
          col('prec').alias('prec'),
          round(sum('prec').over(jourPOby),2).alias('s1'),
          row_number().over(jourPOby).alias('rn1'),
          round(sum('prec').over(jourOby),2).alias('s2'),
          row_number().over(jourOby).alias('rn2'))\
       .show(35)

jourPOby = Window.partitionBy('mois').orderBy('jour')
palmaresM  = Window.partitionBy('mois').orderBy(desc('prec'))
palmaresA  = Window.orderBy(desc('prec'))
meteoMM.where("annee = 2019")\
       .groupBy('mois','jour')\
       .agg( round(sum('precipitations'),2).alias('prec'))\
       .select('mois','jour',
          col('prec').alias('prec'),
          round(sum('prec').over(jourPOby),2).alias('s1'),
          row_number().over(jourPOby).alias('rn1'),
          rank().over(palmaresM).alias('rk1'),
          rank().over(palmaresA).alias('rk2'))\
       .orderBy(desc('prec'))\
       .show(35)

palmaresA  = Window.orderBy("mois")
meteoMM.where("annee = 2019")\
       .groupBy('mois')\
       .agg( round(sum('precipitations'),2).alias('prec'))\
       .select('mois','prec',
          round(sum('prec').over(palmaresA),2).alias('s1'),
          ntile(4).over(palmaresA).alias('ntile'),
          lag('prec',1).over(palmaresA).alias('lag'),
          lead('prec',1).over(palmaresA).alias('lead'))\
       .show(35)
