import org.apache.spark.sql.SparkSession

object donneesMeteo {

  case class MeteoFiltre(station:String,
                   annee:String,
                   mois:String,
                   jour:String,
                   temperature:String,
                   pression:String)

  def parseLineMeteoFiltre(line: String): MeteoFiltre = {
    val fields = line.split(';')

    val meteo: MeteoFiltre = MeteoFiltre(
      fields(0),
      fields(1).substring(0,4),
      fields(1).substring(4,6),
      fields(1).substring(6,8),
      fields(7),
      fields(20))

    return meteo
  }

case class Meteo(station:String,
                   annee:Int,
                   mois:Int,
                   jour:Int,
                   temperature:Double,
                   pression:Int)

  def parseLineMeteo(line: MeteoFiltre): Meteo = {
    val meteo: Meteo = Meteo(line.station,
                             line.annee.toInt,
                             line.mois.toInt,
                             line.jour.toInt,
                             line.temperature.toDouble,
                             line.pression.toInt)

      return meteo
}


val donneesMeteo = spark.sparkContext.textFile("donnees/meteo")
                   .filter( l => l.substring(0,4).matches("^\\d\\d\\d\\d$"))
                   .map(parseLineMeteoFiltre)
                   .filter(l => l.temperature.matches("[\\d.]+"))
                   .filter(l => l.pression.matches("\\d+"))
                   .map(parseLineMeteo)

    println("-"*50)
    donneesMeteo.take(3).foreach(println)
    println("-"*50)

    val donneesMeteoDF = spark.createDataFrame(donneesMeteo)
    donneesMeteoDF.printSchema()
    //donneesMeteoDF.write.parquet("C:\\SolutionsSpark\\donnees\\meteo30_parquet")
    val parquetFileDF = spark.read.parquet("C:\\SolutionsSpark\\donnees\\meteo30_parquet")
    parquetFileDF.createOrReplaceTempView("donneesMeteo")

  }
}
