from pyspark.sql.functions import *

meteoDataFrame.count()
meteoDataFrame.selectExpr(
        'numer_sta','t  - 273.15 as temperature'
        ).limit(5).show()

meteoDataFrame.select('numer_sta').count()
meteoDataFrame.select('numer_sta').distinct().count()
meteoDataFrame.select('numer_sta').distinct().show(3)
meteoDataFrame.distinct().count()

villes.where('latitude > 49').show()
villes.where('latitude > 49 and altitude > 90').show()
villes.where('latitude > 49 or altitude > 800').show()

meteoDataFrame.count()
meteoDataFrame.sample(1/100).count()
meteoDataFrame.sample(True,1/100).count()

villes.sample(True,2/100,0).show()
villes.filter("Id == '07168' or Id == '07280'").show()
villes.filter("Id = '07168' or Id = '07280'").show()

meteoDataFrame.groupBy('numer_sta').show(3)
