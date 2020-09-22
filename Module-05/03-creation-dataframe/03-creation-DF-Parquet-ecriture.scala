import org.apache.spark.sql.functions._

val parseAnnee: (String => Int) = (arg: String) => {arg.substring(0,4).toInt }
val parseMois: (String => Int) = (arg: String) => {arg.substring(4,6).toInt }
val parseJour: (String => Int) = (arg: String) => {arg.substring(6,8).toInt }
val sqlfA = udf(parseAnnee)
val sqlfM = udf(parseMois)
val sqlfJ = udf(parseJour)


val donnees  = spark.read.format("csv").
      option("sep",";").
      option("mergeSchema", "true").
      option("header","true").
      option("nullValue","mq").
      option("inferSchema", "true").
      option("path", "/user/spark/donnees/meteo").
      load().
      withColumn("date", $"date".cast("string"))

val fichier = "/user/spark/donnees/meteo_parquet"
val format  = "parquet"

val donneesMeteo = donnees.
                    withColumn("annee",sqlfA(donnees("date"))).
                    withColumn("mois",sqlfM(donnees("date"))).
                    withColumn("jour",sqlfJ(donnees("date"))).
                    withColumn("t",round(donnees("t") - 273.15,2)).
                    withColumn("u",round(donnees("u"))).
                    withColumn("ff",round(donnees("ff"),2)).
                    withColumn("vv",round(donnees("vv")/1000,2)).
                    withColumn("pres",round(donnees("pres")/1000,2))

donneesMeteo.write.
         mode("overwrite").
         format(format).
         option("path", fichier).
         save()

donneesMeteo.write.
        mode("overwrite").
        format(format).
        bucketBy(100,"annee","mois").
        saveAsTable("meteo_bucketBy")

val fichier = "/user/spark/donnees/meteo_partitionBy_annee_parquet"
donneesMeteo.write.
        mode("overwrite").
        format(format).
        partitionBy("annee").
        option("path", fichier).
        save()



CREATE OR REPLACE TEMPORARY VIEW meteo_annee
USING parquet
OPTIONS ( path "/user/spark/donnees/meteo_partitionBy_annee_parquet" )

:q
