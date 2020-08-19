from pyspark.sql.functions import *

meteoDataFrame.columns
meteoDataFrame.printSchema()

meteoDataFrame.select('numer_sta',
        expr('t  - 273.15').alias('temperature'),
        expr('(t + pres/1000)*vv/1000')
             .alias('calc')).show(3)

villes.select('*',expr('altitude * 1000').alias('alt')).show(3)
villes.selectExpr('*','altitude * 1000 as alt').show(3)

meteoDataFrame.selectExpr('numer_sta',
        expr('t  - 273.15').alias('temperature'),
        expr('(t + pres/1000)*vv/1000')
             .alias('calc')).show(3)

meteo.where('id < 8000')\
     .select('annee','mois_jour',
             'temperature')\
     .describe().show()
