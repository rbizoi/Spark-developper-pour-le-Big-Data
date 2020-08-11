import org.apache.spark.sql.SparkSession

def printConfigs(session: SparkSession) = {
  val mconf = session.conf.getAll
  for (k <- mconf.keySet) {
      if ( k.matches("spark.executor.*") ||
           k.matches("spark.default.parallelism"))
              println(s"${k} -> ${mconf(k)}")
    }
}

spark.conf.set("spark.default.parallelism",8)
printConfigs(spark)



val data = Array(("Ajaccio",653    ),
                 ("Angers",690     ),
                 ("Angoulème",826  ),
                 ("Besançon",1088  ),
                 ("Biarritz",1474  ),
                 ("Bordeaux",947   ),
                 ("Brest",1157     ),
                 ("Caen",713       ),
                 ("Clermont-Fd",571),
                 ("Dijon",734      ),
                 ("Embrun",698     ),
                 ("Grenoble",1005  ),
                 ("Lille",612      ),
                 ("Limoges",910    ),
                 ("Lyon",828       ),
                 ("Marseille",533  ),
                 ("Montpellier",736),
                 ("Nancy",721      ),
                 ("Nantes",809     ),
                 ("Nice",868       ),
                 ("Nîmes",680      ),
                 ("Orléans",621    ),
                 ("Paris",624      ),
                 ("Perpignan",629  ),
                 ("Poitiers",702   ),
                 ("Reims",575      ),
                 ("Rennes",634     ),
                 ("Rouen",716      ),
                 ("St-Quentin",684 ),
                 ("Strasbourg",719 ),
                 ("Toulon",837     ),
                 ("Toulouse",656   ),
                 ("Tours",687      ),
                 ("Vichy",761      ))

val data = Array(("Ajaccio",653    ),
                ("Angoulème",826  ),
                ("Besançon",1088  ),
                ("Biarritz",1474  ),
                ("Bordeaux",947   ),
                ("Brest",1157     ),
                ("Caen",713       ),
                ("Strasbourg",719 ))

val rdd01 = spark.sparkContext.parallelize(data)
val rdd02 = spark.sparkContext.parallelize(data,1)
val rdd03 = spark.sparkContext.parallelize(data,3)

rdd01.partitions
rdd02.partitions
rdd03.partitions

val rdd04 = spark.sparkContext.textFile(
                 "/user/spark/donnees/postesSynop.csv")
rdd04.partitions

val rdd05 = spark.sparkContext.textFile(
                 "/user/spark/donnees/postesSynop.csv", 8)
rdd05.partitions
