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






meteoMM = meteoFance.where("ville = 'Mont De Marsan' and \
                                annee = 2019 and mois = 11 and jour < 11")\
                     .select('jour','temperature','humidite','visibilite',
                             'pression','precipitations')
meteoMM.show()
window = Window.partitionBy()
meteoMM.select('jour','precipitations',
               sum('precipitations').over(window)\
                   .alias("somme totale"))\
        .show(5)



meteoMM.groupBy('jour')\
        .agg( round(sum('precipitations'),2).alias('precipitations'))\
        .orderBy(col('jour'))\
        .rollup('jour')\
        .agg( round(sum('precipitations'),2).alias('precipitations'))\
        .orderBy(col('jour').asc_nulls_last())\
        .show()


total = Window.partitionBy()
jour  = Window.partitionBy('jour')
meteoMM.select('jour','precipitations',
               round(sum('precipitations').over(jour),2)\
                   .alias("somme journalière"),\
               sum('precipitations').over(total)\
                   .alias("somme totale"))\
        .show()


meteoMM = meteoFance.where("ville = 'Mont De Marsan'")\
           .select('ville','annee','mois',
                           'jour','temperature','precipitations')\
           .groupBy('ville', 'annee', 'mois', 'jour')\
           .agg( round(avg('temperature'),2).alias('temperature'),
                round(sum('precipitations'),2).alias('precipitations'))\
           .select('annee','mois','jour','temperature','precipitations')
meteoMM.show(5)

jour          = Window.partitionBy('jour')
mois          = Window.partitionBy('mois')
moisAnnee     = Window.partitionBy('mois','annee')
annee         = Window.partitionBy('annee')
jourMois      = Window.partitionBy('jour','mois')
jourMoisAnnee = Window.partitionBy('jour','mois','annee')

meteoMM.select('jour','mois','annee',col('precipitations').alias('prec'),
          round(sum('precipitations').over(jourMoisAnnee),2).alias('s1'),
          round(sum('precipitations').over(jourMois),2).alias('s2'),
          round(sum('precipitations').over(moisAnnee),2).alias('s3'),
          round(sum('precipitations').over(mois),2).alias('s4'),
          round(sum('precipitations').over(annee),2).alias('s5'),
          round(sum('precipitations').over(jour),2).alias('s6'))\
       .show(28)

meteoMM.where('annee = 1997 and mois = 12 and jour = 31').agg(sum('precipitations').alias('s1')).show()
meteoMM.where('mois = 12 and jour = 31').agg(sum('precipitations').alias('s2')).show()
meteoMM.where('annee = 1996 and mois = 12').agg(sum('precipitations').alias('s3')).show()
meteoMM.where('mois  = 12'  ).agg(sum('precipitations').alias('s4')).show()
meteoMM.where('annee = 1996').agg(sum('precipitations').alias('s5')).show()
meteoMM.where('jour = 31').agg(sum('precipitations').alias('s6')).show()
meteoMM.agg(sum('precipitations')).show()

#meteoMM = meteoFance.where("ville = 'Mont De Marsan' \
#                            and annee = 2019")\
#           .select('ville','annee','mois',
#                           'jour','temperature','precipitations')\
#           .groupBy('ville', 'annee', 'mois', 'jour')\
#           .agg( round(avg('temperature'),2).alias('temperature'),
#                round(sum('precipitations'),2).alias('precipitations'))\
#           .select('mois','jour','temperature','precipitations')\
#           .orderBy('mois','jour')
#meteoMM.show(5)
jour    = Window.partitionBy('mois')
jourOby = Window.partitionBy('mois').orderBy('jour')
meteoMM.where("annee = 2019")\
       .select('mois','jour',
          col('temperature').alias('temp'),
          round(avg('temperature').over(jourOby),2).alias('s1'),
          round(avg('temperature').over(jour),2).alias('s2'),
          col('precipitations').alias('prec'),
          round(sum('precipitations').over(jourOby),2).alias('s3'),
          round(sum('precipitations').over(jour),2).alias('s4'))\
       .show(32)


jourPOby = Window.partitionBy('mois').orderBy('jour')
jourOby  = Window.orderBy('mois','jour')
meteoMM.where("annee = 2019")\
       .select('mois','jour',
          col('precipitations').alias('prec'),
          round(sum('precipitations').over(jourPOby),2).alias('s3'),
          round(sum('precipitations').over(jourOby),2).alias('s4'))\
       .show(35)


jourOby = Window.partitionBy('mois')\
                .orderBy('jour')\
                .rowsBetween(Window.unboundedPreceding,Window.currentRow)

meteoMM.where("annee = 2019")\
       .select('mois','jour',
          col('precipitations').alias('prec'),
          round(sum('precipitations')\
                   .over(jourOby),2).alias('s3'))\
       .show(32)


jour = Window.orderBy('mois').rowsBetween(-1, 1)
meteoMM.where('annee = 2019')\
       .groupBy('mois')\
       .agg( round(avg('temperature'),2).alias('temp'),
             round(sum('precipitations'),2).alias('prec'))\
       .select('mois','temp',
          round(avg('temp').over(jour),2).alias('s1'),
          'prec',
          round(sum('prec').over(jour),2).alias('s2'))\
       .show(32)
