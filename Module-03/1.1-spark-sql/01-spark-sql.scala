import org.apache.spark.sql.functions._

val parseVille: (String => String) = (arg: String) => {arg.toLowerCase.split(' ').
                map(_.capitalize).mkString(" ").
                split('-').map(_.capitalize).mkString(" ") }
val sqlfV = udf(parseVille)


val donneesStations = spark.read.json("donnees/json/postesSynop.json")
val postesMeteo     = donneesStations.
                       withColumn("Nom", sqlfV(donneesStations("Nom")))
postesMeteo.createOrReplaceTempView("PostesMeteo")
spark.sql("select * from PostesMeteo limit 3").show()
spark.sql("show databases").show()
spark.sql("use cours_spark")
spark.sql("show tables").show()
spark.sql("DROP TABLE IF EXISTS cours_spark.postesmeteo")
postesMeteo.write.saveAsTable("postesmeteo")
spark.sql("show tables").show()
spark.sql("select * from cours_spark.postesmeteo where ID < 8000 limit 3").show()
