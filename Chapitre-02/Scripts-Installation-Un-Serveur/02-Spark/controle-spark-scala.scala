import org.apache.spark.sql.functions._

val parseAnnee: (String => Int) = (arg: String) => {arg.substring(0,4).toInt }
val parseMois: (String => Int) = (arg: String) => {arg.substring(4,6).toInt }
val parseJour: (String => Int) = (arg: String) => {arg.substring(6,8).toInt }
val parseVille: (String => String) = (arg: String) => {arg.toLowerCase.split(' ').map(_.capitalize).mkString(" ").split('-').map(_.capitalize).mkString(" ") }
val sqlfA = udf(parseAnnee)
val sqlfM = udf(parseMois)
val sqlfJ = udf(parseJour)
val sqlfV = udf(parseVille)


val donneesStations = spark.read.format("csv").option("sep", ";").option("mergeSchema", "true").option("header","true").option("nullValue","mq").load("/user/spark/donnees/postesSynop.csv").filter("ID<8000").toDF("Station","Ville","Latitude","Longitude","Altitude")
val donneesS = donneesStations.withColumn("Ville", sqlfV(donneesStations("Ville")))

donneesS.printSchema()
donneesS.show()

val donneesMeteo=spark.read.format("csv").option("sep",";").option("mergeSchema","true").option("header","true").option("nullValue","mq").load("/user/spark/donnees/meteo").filter("numer_sta<8000").select("numer_sta","date","t","u","ff","vv","pres")
val donneesM=donneesMeteo.withColumn("Annee",sqlfA(donneesMeteo("date"))).withColumn("Mois",sqlfM(donneesMeteo("date"))).withColumn("Jour",sqlfJ(donneesMeteo("date"))).withColumn("Temperature",round(donneesMeteo("t")-273.15,2)).withColumn("Humidite",round(donneesMeteo("u"))).withColumn("VitesseVent",round(donneesMeteo("ff"),2)).withColumn("Visibilite",round(donneesMeteo("vv")/1000,2)).withColumn("Pression",round(donneesMeteo("pres")/1000,2)).withColumn("Station",donneesMeteo("numer_sta")).select("Station","Annee","Mois","Jour","Temperature","Humidite","VitesseVent","Visibilite","Pression")

donneesM.printSchema()
donneesM.show()

spark.sql("use cours_spark")
spark.sql("DROP TABLE IF EXISTS cours_spark.meteoComplet")
donneesS.join(donneesM,donneesS.col("Station").equalTo(donneesM.col("Station"))).select("Ville","Latitude","Longitude","Altitude","Annee","Mois","Jour","Temperature","Humidite","VitesseVent","Visibilite","Pression").write.saveAsTable("meteoComplet")

spark.sql("select * from cours_spark.meteoComplet").show()
spark.sql("select * from cours_spark.meteoComplet").groupBy("Ville").pivot("Mois").agg(round(avg("Temperature"),2)).sort("Ville").toDF("Ville","Janvier","Fevrier","Mars","Avril","Mai","Juin","Juillet","Aout","Septembre","Octobre","Novembre","Decembre").write.saveAsTable("meteoMensuelle")
spark.sql("select * from cours_spark.meteoMensuelle order by 1").show(48)


spark.sql("select * from cours_spark.meteoMensuelle order by 1").createOrReplaceGlobalTempView("donneesMeteo")
spark.sql("select * from global_temp.donneesMeteo").show(false)
