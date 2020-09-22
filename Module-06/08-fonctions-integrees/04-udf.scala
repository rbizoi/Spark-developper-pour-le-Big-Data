import org.apache.spark.sql.functions._

val modifNomVille:(String => String) = (arg: String) => {
        if (arg != null)
           arg.toLowerCase.split(' ').
               map(_.capitalize).
               mkString(" ").
               split('-').
               map(_.capitalize).
               mkString(" ")
        else
           " "
      }

val nomVille = udf(modifNomVille)

villes.select('ville, nomVille('ville)).show()


val donneesStations = spark.read.format("csv").option("sep", ";").option("mergeSchema", "true").option("header","true").option("nullValue","mq").load("/user/spark/donnees/postesSynop.csv").filter("ID<8000").toDF("Station","Ville","Latitude","Longitude","Altitude")
val donneesS = donneesStations.withColumn("Ville", sqlfV(donneesStations("Ville")))
villes.
