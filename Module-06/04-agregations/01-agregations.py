from pyspark.sql.functions import *


meteo.where('id < 8000')\
     .select('annee','mois_jour','temperature','precipitations')\
     .describe().show()

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
            round(avg('pression'),2).alias('pression'),
            round(sum('pression')).alias('precipitations'))\
     .orderBy("id","annee")\
     .show(28)

meteo.where('id < 8000')\
     .groupBy('annee','mois')\
     .agg(round(sum('pression') / 1000).alias('precipitations'))\
     .orderBy('annee','mois')\
     .show(5)

meteo.where('id < 8000')\
     .groupBy('annee','mois')\
     .agg(round(sum('pression') / 1000).alias('precipitations'))\
     .rollup('annee','mois')\
     .agg( round(sum('precipitations')).alias('precipitations'))\
     .orderBy(col('annee').asc_nulls_last(),
              col('mois').asc_nulls_last())\
     .show(14)


meteo.where('id < 8000')\
     .groupBy('annee','mois')\
     .agg(round(sum('pression') / 1000).alias('precipitations'))\
     .count()

meteo.where('id < 8000')\
     .groupBy('annee','mois')\
     .agg(round(sum('pression') / 1000).alias('precipitations'))\
     .rollup('annee','mois')\
     .agg( round(sum('precipitations')).alias('precipitations'))\
     .count()

meteo.where('id < 8000')\
     .groupBy('annee','mois')\
     .agg(round(sum('pression') / 1000).alias('precipitations'))\
     .cube('annee','mois')\
     .agg( round(sum('precipitations')).alias('precipitations'))\
     .count()

meteo.where('id < 8000')\
     .groupBy('annee','mois')\
     .agg(round(sum('pression') / 1000).alias('precipitations'))\
     .cube('annee','mois')\
     .agg( round(sum('precipitations')).alias('precipitations'))\
     .orderBy(col('annee'),col('mois'))\
     .show(24)

meteo.where('id < 8000')\
     .groupBy('annee','mois')\
     .agg(round(sum('pression') / 1000).alias('precipitations'))\
     .rollup('annee','mois')\
     .agg(round(sum('precipitations')).alias('precipitations'))\
     .show(2000)

meteo.where('id < 8000')\
     .rollup('annee','mois')\
     .agg(
            round(sum('precipitations')).alias('precipitations'))\
     .orderBy(col('annee').asc_nulls_last(),
              col('mois').asc_nulls_last())\
     .show(16)

meteo.where('id < 8000')\
     .rollup('annee','mois')\
     .agg(
            round(avg('temperature'),2).alias('temperature'),
            round(avg('humidite'),2).alias('humidite'),
            round(avg('visibilite'),2).alias('visibilite'),
            round(avg('pression'),2).alias('pression'),
            round(avg('pression'),2).alias('precipitations'))\
     .orderBy(col('annee').asc_nulls_last(),
              col('mois').asc_nulls_last())\
     .show(16)

meteo.where('id < 8000')\
     .groupBy('id','annee')\
     .agg(
            {'id':'count',
            'temperature':'avg',
            'humidite':'avg'}
     ).toDF('id','annee','humidite','temperature','nb_villes').show(10)


meteo.where('id < 8000 and annee > 2014')\
     .groupBy('id','annee')\
     .agg( round(avg('temperature'),2).alias('temperature'))\
     .orderBy("id","annee")\
     .show(10)

meteo.where('id < 8000 and annee > 2014')\
      .groupBy('id')\
      .pivot('annee')\
      .agg( round(avg('temperature'),2))\
      .sort('id')\
      .show(10)

villes.select('ville',
               round('altitude',-2).alias('altitude'))\
      .groupBy('altitude')\
      .agg(collect_list('ville').alias('ville par altitude'))\
.show(truncate=False)
