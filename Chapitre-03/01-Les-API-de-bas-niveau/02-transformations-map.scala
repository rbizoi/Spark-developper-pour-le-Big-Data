
val rdd05 = spark.sparkContext.textFile(
                  "/user/spark/donnees/postesSynop.csv", 8)
rdd05.take(2)








def printConfigs(session: SparkSession) = {
  val mconf = session.conf.getAll
  for (k <- mconf.keySet) {
      if ( k.matches("spark.executor.*") ||
           k.matches("spark.default.parallelism"))
              println(s"${k} -> ${mconf(k)}")
    }
}






case class stationMeteo(station:String,
                        nom:String,
                        latitude:Double,
                        longitude:Double,
                        altitude:Int)


def transformLignePoste(line: String): stationMeteo = {
  val fields = line.split(';')
  val station: stationMeteo = stationMeteo(
    fields(0),
    fields(1).toLowerCase.split(' ').map(_.capitalize).mkString(" ").split('-').map(_.capitalize).mkString(" "),
    fields(2).toDouble,
    fields(3).toDouble,
    fields(4).toInt)
  return station
}

val parseVille: (String => String) = (arg: String) => {
               arg.toLowerCase.split(' ').
                map(_.capitalize).mkString(" ").
                split('-').map(_.capitalize).mkString(" ") }
val sqlfV = udf(parseVille)


 val rdd06 = rdd01.map(x => x(0))



 donnees05 = donnees04.filter( lambda ligne :
                          str(ligne)[0].isdigit() ).\
                          persist()

donnees11 = donnees10.filter(donnees10.Id < '08000')
fields(1).substring(0,5),



def transformLignePoste(ligne):
    champs = ligne.split(";")
    return ( str(champs[0]),
            (str(champs[1]).title(),
            float(champs[2]),
            float(champs[3]),
            int(champs[4])))



rdd06 = rdd05.map(transformLignePoste)
rdd06 = rdd05.map( lambda ligne :
                   transformLignePoste(ligne)).\
                   persist()

donnees07 = donnees06.join(donnees03).persist()

donnees08 = donnees07.sortByKey().persist()

donnees09 = donnees08.map(lambda ligne : tuple([ligne[0]] +
                           [x for x in ligne[1][0]] +
                           [x for x in ligne[1][1]]) ).persist()


rdd06 = rdd05.filter( lambda ligne :
                                                    str(ligne)[0].isdigit() ).\
                                                    persist()
