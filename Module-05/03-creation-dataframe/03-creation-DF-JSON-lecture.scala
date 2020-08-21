import org.apache.spark.sql.types._
val customSchema = StructType(
   StructField("etat"            , IntegerType , true)::
   StructField("etat_descriptif" , StringType , true)::
   StructField("ident"           , StringType, true)::
   StructField("idsurfs"         , StringType, true)::
   StructField("infousager"      , StringType, true)::
   StructField("libre"           , IntegerType, true)::
   StructField("nom_parking"     , StringType, true)::
   StructField("total"           , IntegerType  , true)::Nil)

val parkingStras01  = spark.read.format("json").
      option("mergeSchema", "true").
      option("inferSchema", "true").
      load("/user/spark/donnees/json/parking_stras_json").
      cache()

parkingStras01.printSchema()

val parkingStras02  = spark.read.format("json").
      option("mergeSchema", "true").
      schema(customSchema).
      load("/user/spark/donnees/json/parking_stras_json").
      cache()

parkingStras02.printSchema()
parkingStras02.select ("nom_parking","libre","total").show(3)


url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=vente_a_la_ferme&q=&lang=fr&rows=39"
import org.apache.spark.sql.{DataFrame, SparkSession}

def GetUrlContentJson(url: String): DataFrame ={
    val result = scala.io.Source.fromURL(url).mkString
    val jsonResponseOneLine = result.toString().stripLineEnd
    val jsonRdd = spark.createDataset(jsonResponseOneLine :: Nil)
    val jsonDf = spark.read.json(jsonRdd)
    return jsonDf
}
