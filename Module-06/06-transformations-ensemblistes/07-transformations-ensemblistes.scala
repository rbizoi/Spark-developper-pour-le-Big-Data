import org.apache.spark.sql.functions._

dfa.union(dfb).orderBy("ville").show(40)
dfa.select("ville").union(dfb.select("ville")).distinct().orderBy("ville").show(40)


meteo.select("id","temperature").
     union(villes.select("ville","altitude")).
     orderBy("id").show(5)

meteo.select("id","temperature").
     union(villes.select("ville","altitude")).
     orderBy(desc("id")).show(5)

dfa.select("ville").intersect(dfb.select("ville")).show()
