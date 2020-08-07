var lines = spark.sparkContext.textFile("donnees/livres")

val transfLines = lines.map(x => x.toLowerCase).
         map(x => x.replaceAll("[^\\p{L}]", " "))

val mots = transfLines.flatMap(x => x.split(" "))
val motsAvec1 = mots.map(t => (t, 1))
val motsOccurences = motsAvec1.reduceByKey((a, b) => a + b)
val motsFiltres = motsOccurences.filter(w => w._1.length() > 3)
val occurecesMots = motsFiltres.reduceByKey((a, b) => (b,a))
val motsOrdonnees = motsFiltres.sortByKey(ascending = false)
val sortie = motsOrdonnees.collect()
