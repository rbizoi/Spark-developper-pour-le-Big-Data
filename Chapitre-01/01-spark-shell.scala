import org.apache.spark.sql.functions._
val parseVille: (String => String) = (arg: String) =>
     {arg.toLowerCase.split(' ').
     map(_.capitalize).mkString(" ").
     split('-').map(_.capitalize).mkString(" ") }

val sqlfV = udf(parseVille)

val donneesStations = spark.read.format("csv").
         option("sep", ";").option("mergeSchema", "true").
         option("header","true").option("nullValue","mq").
         load("file:/home/spark/postesSynop.csv").
         filter("ID<8000").toDF("Station","Ville",
         "Latitude","Longitude","Altitude")

val donneesS = donneesStations.withColumn("Ville",
                   sqlfV(donneesStations("Ville")))
:q
