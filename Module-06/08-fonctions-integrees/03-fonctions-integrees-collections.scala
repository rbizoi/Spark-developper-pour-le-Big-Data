import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
val url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=vente_a_la_ferme&q=&lang=fr&rows=39"

def lireUnJsonURL(url: String): DataFrame ={
   val chaine = scala.io.Source.fromURL(url).mkString
   val json = chaine.toString().stripLineEnd
   val jsonRdd = spark.createDataset(json :: Nil)
   val jsonDf = spark.read.json(jsonRdd)
   return jsonDf
}

val donnees = lireUnJsonURL(url)
donnees.show()



donnees.show()
donnees.printSchema()

donnees.select(  'nhits,'parameters,
                 'records.getField("fields").as("fields"),
                 'records.getField("geometry").as("geometry"),
               ).show()

donnees.select( "nhits","parameters.*","records.fields","records.geometry"
             ).show()

donnees.select( 'records.getField("fields").getField("nom").as("nom"),
                 'records.getField("fields").getField("commune").as("commune"),
                 'records.getField("fields").getField("code_postal").as("code_postal")
               ).show()

donnees.select( 'records.getField("fields").getField("nom").getItem(0).as("nom"),
                'records.getField("fields").getField("commune").getItem(0).as("commune"),
                'records.getField("fields").getField("code_postal").getItem(0).as("code_postal")
              ).show()


donnees.show()
donnees.select('nhits,'parameters,
                explode('records.getField("fields")).alias("fields")).
        show()

val fermes = donnees.select( explode('records).alias("records")).
                select("records.*").
                select("recordid","fields.*","geometry","record_timestamp")
fermes.printSchema()
fermes.select("nom","adresse","code_postal","commune","famille").show()
