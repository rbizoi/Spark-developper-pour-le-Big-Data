from pyspark.sql.functions import *

meteoDataFrame.columns
meteoDataFrame.printSchema()

meteoDataFrame.select('numer_sta',
        expr('t  - 273.15').alias('temperature'),
        expr('(t + pres/1000)*vv/1000')
             .alias('calc')).show(3)

villes.select('*',expr('altitude * 1000').alias('alt')).show(3)
villes.selectExpr('*','altitude * 1000 as alt').show(3)

villes.drop('Id', 'latitude', 'longitude').show(3)

villes.drop('Id', 'latitude', 'longitude')\
       .orderBy('ville').show(5)

villes.drop('Id', 'latitude', 'longitude')\
       .orderBy(desc('altitude')).show(5)

meteo.where('id < 8000')\
      .select ('id','annee','mois','jour','temperature')\
      .orderBy( 'id','annee','mois','jour','temperature',
                ascending=[1,0,0,0,1])\
      .show(5)

meteo.where('id < 8000')\
     .select('annee','mois_jour',
             'temperature')\
     .describe().show()




            "humidite":"avg"}
     ).toDF("id","annee","temperature").show(10)



meteo.where('id < 8000 and annee > 2014')\
     .groupBy('id','annee')\
     .agg( round(avg('temperature'),2).alias('temperature'))\
