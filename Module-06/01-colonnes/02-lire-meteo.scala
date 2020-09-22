import org.apache.spark.sql.functions._

val meteoDataFrame  = spark.read.format("csv").
    option("sep",";").
    option("header","true").
    option("nullValue","mq").
    option("inferSchema", "true").
    load("donnees/meteo").
    cache()
meteoDataFrame.columns
meteoDataFrame.printSchema()
meteoDataFrame.col("date")
meteoDataFrame.column("date")
meteoDataFrame.select("date","t","dd","ff","pres").show(5)
meteoDataFrame.select("t" -273.15).show(3)

meteoDataFrame.select(col("numer_sta"),col("t") - 273.15).show(3)
meteoDataFrame.select($"numer_sta",$"t" - 273.15).show(3)
meteoDataFrame.select('numer_sta,'t - 273.15).show(3)

meteoDataFrame.select('numer_sta,
        expr("t  - 273.15").alias("temperature"),
        expr("(t + pres/1000)*vv/1000")
             .alias("calc")).show(3)


meteoDataFrame.select(
      'numer_sta,
      'date,
      ('date === 20200107000000L).alias("date_01072020"),
      'dd,
      ('dd  >= 80).alias("dd_80+"),
      expr("t  - 273.15").alias("temperature")).show(5)


meteoDataFrame.
  withColumn("Temperature",round('t - 273.15, 2)).
  withColumn("Humidite",round('u / 100, 4)).
  withColumn("VitesseVent",round('ff , 2)).
  withColumn("Visibilite",round('vv / 1000, 2)).
  withColumn("Pression",round('pres / 1000 )).
  select ("Temperature","Humidite","VitesseVent","Visibilite","Pression").
  show(3)
