from pyspark.sql.functions import expr,col

meteoDataFrame  = spark.read.format('csv')\
    .option('sep',';')\
    .option('header','true')\
    .option('nullValue','mq')\
    .option('inferSchema', 'true')\
    .load('donnees/meteo')\
    .cache()

meteoDataFrame.select('numer_sta',"date","t").show(3)
meteoDataFrame.numer_sta
meteoDataFrame["numer_sta"]
meteoDataFrame.t
meteoDataFrame.select('numer_sta',meteoDataFrame.t - 273.15).show(3)
meteoDataFrame.select('numer_sta',col('t') - 273.15).show(3)

meteoDataFrame.select('numer_sta',
        expr('t  - 273.15').alias('temperature'),
        expr('(t + pres/1000)*vv/1000')
             .alias('calc')).show(3)
