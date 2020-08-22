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





val meteoFance = meteo.where("id < 8000").
             join( villes.withColumnRenamed("Id", "id"),"id").
             select(initcap(regexp_replace('ville,"-"," ")).alias("ville"),
                     'annee,'mois,'jour,'temperature,
                     'humidite,'visibilite,'pression,'precipitations)

meteoFance.where("ville = 'Mont De Marsan' and "+
                 "annee = 2019 and mois = 11 and jour < 11").
           selectExpr("ville","annee","mois","jour","temperature as t",
                      "humidite as h","visibilite as v",
                      "pression as p","precipitations as e").
           show()

val meteoMM = meteoFance.where("ville = 'Mont De Marsan' and "+
                                "annee = 2019 and "+
                                "mois = 11 and jour < 11").
                         select("jour","temperature",
                                "humidite","visibilite",
                                "pression","precipitations")
meteoMM.show()
meteoMM.groupBy("jour").
        agg( round(sum("precipitations"),2).alias("precipitations")).
        orderBy(col("jour")).
        rollup("jour").
        agg( round(sum("precipitations"),2).alias("precipitations")).
        orderBy(col("jour").asc_nulls_last).
        show()

meteoMM.agg(sum("precipitations")).show()






val meteoFance = meteo.where("id < 8000").
             join( villes.withColumnRenamed("Id", "id"),"id").
             select(initcap(regexp_replace('ville,"-"," ")).alias("ville"),
                     'annee,'mois,'jour,'temperature,
                     'humidite,'visibilite,'pression,'precipitations)

meteoFance.where("ville = 'Mont De Marsan' and "+
                 "annee = 2019 and mois = 11 and jour < 11").
           selectExpr("ville","annee","mois","jour","temperature as t",
                      "humidite as h","visibilite as v",
                      "pression as p","precipitations as e").
           show()

val meteoMM = meteoFance.where("ville = 'Mont De Marsan' and "+
                                "annee = 2019 and "+
                                "mois = 11 and jour < 11").
                         select("jour","temperature",
                                "humidite","visibilite",
                                "pression","precipitations")
















from pyspark.sql import Window
window = Window.partitionBy()
meteoFance.where("ville = 'Strasbourg Entzheim'\
                 and annee = 2020 and mois = 7")\
          .select('annee','mois','jour','temperature',
                   avg('temperature').over(window)).show()




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



meteo.select('id','precipitations1','precipitations3','precipitations6',
      'precipitations12','precipitations24','temperature').describe().show()


meteo.select( coalesce('precipitations3','precipitations24',
                       'precipitations12','precipitations6',
                       'precipitations1').alias('precipitations')).show()


meteo.select( coalesce('precipitations24','precipitations12',
                       'precipitations6','precipitations3',
                       'precipitations1').alias('precipitationsH')).describe().show()


meteo.count(),meteo.where(meteo['precipitations3'].isnull()).count()
