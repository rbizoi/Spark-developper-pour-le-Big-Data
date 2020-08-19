from pyspark.sql.functions import *

meteo.where('id < 8000')\
     .select('annee','mois_jour','temperature')\
     .describe().show()

meteo.where('id < 8000').count()

meteo.where('id < 8000')\
     .select('humidite','visibilite','pression')\
     .describe().show()



meteo.where('id < 8000')\
     .groupBy('annee')\
     .avg('temperature','visibilite','pression').show(5)

meteo.where('id < 8000')\
     .groupBy('id','annee')\
     .max('temperature','visibilite','pression').show(5)


meteo.where('id < 8000')\
     .groupBy('id','annee')\
     .agg(
            count('id').alias('nb_villes'),
            round(avg('temperature'),2).alias('temperature'),
            round(avg('humidite'),2).alias('humidite'),
            round(avg('visibilite'),2).alias('visibilite'),
            round(avg('pression'),2).alias('pression')
     ).show(10)

meteo.where('id < 8000')\
     .groupBy('id','annee')\
     .agg(
            {"id":"count",
            "temperature":"avg",
            "humidite":"avg"}
     ).toDF("id","annee","humidite","temperature","nb_villes").show(10)

     
