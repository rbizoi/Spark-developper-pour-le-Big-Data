//------------------------------------------------------------------
// 1.2.	Transformations
//------------------------------------------------------------------

//------------------------------------------------------------------
// 1.2.1.	filter
//------------------------------------------------------------------
val rdd05 = spark.sparkContext.textFile(
                  "/user/spark/donnees/postesSynop.csv", 8)
rdd05.take(2).foreach(println)
rdd05.count()
val rdd06 = rdd05.filter( l => l.substring(0,5).matches("^\\d+$"))
rdd06.take(2).foreach(println)
rdd06.count()
val rdd07 = rdd06.filter( l => l.substring(0,5) < "08000")
rdd07.take(2).foreach(println)
rdd07.count()

//------------------------------------------------------------------
// 1.2.2.	map
//------------------------------------------------------------------
rdd01.take(3).foreach(println)
val rdd08 = rdd01.map(x => ( x._1.toUpperCase,x._2 * 10 ) )
rdd08.take(3).foreach(println)

case class stationMeteo(station:String,
                        nom:String,
                        latitude:Double,
                        longitude:Double,
                        altitude:Int)

def transformLignePoste(line: String): stationMeteo = {
  val fields = line.split(';')
  val station: stationMeteo = stationMeteo(
    fields(0),
    fields(1).toLowerCase.split(' ').
              map(_.capitalize).mkString(" ").
              split('-').
              map(_.capitalize).
              mkString(" "),
    fields(2).toDouble,
    fields(3).toDouble,
    fields(4).toInt)
  return station
}

val rdd09 = rdd07.map(transformLignePoste)
rdd09.take(3).foreach(println)

flatMap(lambda x: range(0, x)
rdd01.take(3).foreach(println)
//------------------------------------------------------------------
// 1.2.3.	flatMap
//------------------------------------------------------------------
val rdd10 = spark.sparkContext.parallelize(Array(1,2,3))
val rdd11 = rdd10.flatMap(x => x.to(3))
rdd11.collect().foreach(println)

val rdd12 = spark.sparkContext.parallelize(
            Array("Heureux qui, comme Ulysse, a fait un beau voyage")).
                   map(x => x.toLowerCase).
                   map(x => x.replaceAll("[^\\p{L}]", " ")).
                   map(x => x.replaceAll("  ", " "))
val rdd13 = rdd12.flatMap(x => x.split(' '))
rdd13.collect().foreach(println)
//------------------------------------------------------------------
// 1.2.4.	intersect
//------------------------------------------------------------------
val rdd14 = spark.sparkContext.parallelize(
                                 Array(1,2,3,4,5,6))
val rdd15 = spark.sparkContext.parallelize(
                                 Array(6,7,8,4,5,6))
rdd14.intersection(rdd15).collect().foreach(println)
//------------------------------------------------------------------
// 1.2.5.	union
//------------------------------------------------------------------
val rdd14 = spark.sparkContext.parallelize(
                                 Array(1,2,3,4,5,6))
val rdd15 = spark.sparkContext.parallelize(
                                 Array(6,7,8,4,5,6))
rdd14.union(rdd15).collect().foreach(println)
//------------------------------------------------------------------
// 1.2.5.	distinct
//------------------------------------------------------------------
rdd14.union(rdd15).distinct().collect().foreach(println)
//------------------------------------------------------------------
// 1.2.7.	cartesian
//------------------------------------------------------------------
def transformLignePoste(line: String) = {
  val champs = line.split(";")
  val c1     = champs(0)
  val c2     = champs(1).toLowerCase.split(' ').
                          map(_.capitalize).mkString(" ").
                          split('-').
                          map(_.capitalize).
                          mkString(" ")
  (c1,c2)}

val villes = spark.sparkContext.textFile(
               "/user/spark/donnees/postesSynop.csv").
               filter( l => l.substring(0,5).matches("^\\d+$")).
               map(transformLignePoste)
villes.take(5).foreach(println)
villes.count()

def transformLigneMeteo(line: String) = {
  val champs = line.split(";")
  val c1 = champs(0)
  val c2 = champs(7).toDouble - 273.15
  (c1,c2)}

val meteo = spark.sparkContext.
       textFile("/user/spark/donnees/meteo/synop.202007.csv").
       filter(ligne => ligne.substring(0,5).matches("^\\d+$")).
       map(ligne => ligne.replaceAll("mq","0")).
       map(transformLigneMeteo)
meteo.take(5).foreach(println)
meteo.count()
villes.cartesian(meteo).take(3).foreach(println)
villes.cartesian(meteo).count()

//------------------------------------------------------------------
// 1.2.8.	RDD (clé,valeur)
//------------------------------------------------------------------
def transformLignePoste(line: String) = {
  val champs = line.split(";")
  val c1     = champs(0)
  val c2     = champs(1).toLowerCase.split(' ').
                          map(_.capitalize).mkString(" ").
                          split('-').
                          map(_.capitalize).
                          mkString(" ")
  val c3     = champs(2).toDouble
  val c4     = champs(3).toDouble
  val c5     = champs(4).toInt
  (c1,(c2,c3,c4,c5))
}

val villes = spark.sparkContext.textFile(
               "/user/spark/donnees/postesSynop.csv").
               filter( l => l.substring(0,5).matches("^\\d+$")).
               map(transformLignePoste)
villes.take(5).foreach(println)

def transformLigneMeteo(line: String) = {
  val champs = line.split(";")
  val c1 = champs(0)
  val c2 = champs(7).toDouble - 273.15
  (c1,c2)
}
val meteo = spark.sparkContext.
       textFile("/user/spark/donnees/meteo/synop.202007.csv").
       filter(ligne => ligne.substring(0,5).matches("^\\d+$")).
       map(ligne => ligne.replaceAll("mq","0")).
       map(transformLigneMeteo)
meteo.take(5).foreach(println)
//------------------------------------------------------------------
// 1.2.9.	groupByKey
//------------------------------------------------------------------
meteo.groupByKey().take(1).foreach(println)
//------------------------------------------------------------------
// 1.2.10.	reduceByKey
//------------------------------------------------------------------
meteo.reduceByKey( (total,valeur) => total+valeur).take(10).foreach(println)

//------------------------------------------------------------------
// 1.2.11.	sortByKey
//------------------------------------------------------------------
def transformLignePoste(line: String) = {
  val champs = line.split(";")
  val c1     = champs(0)
  val c2     = champs(1).toLowerCase.split(' ').
                          map(_.capitalize).mkString(" ").
                          split('-').
                          map(_.capitalize).
                          mkString(" ")
  val c3     = champs(2).toDouble
  val c4     = champs(3).toDouble
  val c5     = champs(4).toInt
  (c2,(c1,c3,c4,c5))
}
val villes = spark.sparkContext.textFile(
               "/user/spark/donnees/postesSynop.csv").
               filter( l => l.substring(0,5).matches("^\\d+$")).
               map(transformLignePoste)
villes.take(5).foreach(println)
villes.sortByKey().take(10).foreach(println)
villes.map( l =>
   ((l._2._4,l._1),(l._2._1,l._2._2,l._2._3))).
   take(3).foreach(println)
villes.map( l =>
      ((l._2._4,l._1),(l._2._1,l._2._2,l._2._3))).
      sortByKey().take(12).foreach(println)
//------------------------------------------------------------------
// 1.2.12.	join
//------------------------------------------------------------------
def transformLignePoste(line: String) = {
  val champs = line.split(";")
  val c1     = champs(0)
  val c2     = champs(1).toLowerCase.split(' ').
                          map(_.capitalize).mkString(" ").
                          split('-').
                          map(_.capitalize).
                          mkString(" ")
  val c3     = champs(2).toDouble
  val c4     = champs(3).toDouble
  val c5     = champs(4).toInt
  (c1,(c2,c3,c4,c5))
}
val villes = spark.sparkContext.textFile(
               "/user/spark/donnees/postesSynop.csv").
               filter( l => l.substring(0,5).matches("^\\d+$")).
               map(transformLignePoste)
villes.sortByKey().take(5).foreach(println)

def transformLigneMeteo(line: String) = {
  val champs = line.split(";")
  val c1 = champs(0)
  val c2 = champs(1).substring(6,8).toInt
  val c3 = champs(7).toDouble
  val c4 = champs(20).toInt
  (c1,(c2,c3,c4))
}

val meteo = spark.sparkContext.
       textFile("/user/spark/donnees/meteo/synop.202007.csv").
       filter(ligne => ligne.substring(0,5).matches("^\\d+$")).
       map(ligne => ligne.replaceAll("mq","0")).
       map(transformLigneMeteo)
meteo.sortByKey().take(5).foreach(println)


villes.join(meteo).sortByKey().take(5).foreach(println)

//------------------------------------------------------------------
// 1.3.	Actions
//------------------------------------------------------------------

def transformLigneMeteo(line: String) = {
  val champs = line.split(";")
  val c1 = champs(0)
  val c2 = champs(1).substring(6,8).toInt
  val c3 = champs(7).toDouble
  val c4 = champs(20).toInt
  (c1,(c2,c3,c4))
}

val meteo = spark.sparkContext.
       textFile("/user/spark/donnees/meteo/synop.202007.csv").
       filter(ligne => ligne.substring(0,5).matches("^\\d+$")).
       map(ligne => ligne.replaceAll("mq","0")).
       map(transformLigneMeteo)

meteo.sortByKey().count()
meteo.sortByKey().take(2)
meteo.sortByKey().take(2).foreach(println)
meteo.sortByKey().countByKey().take(5).foreach(println)
meteo.saveAsTextFile("/user/spark/donnee/meteo072020")
