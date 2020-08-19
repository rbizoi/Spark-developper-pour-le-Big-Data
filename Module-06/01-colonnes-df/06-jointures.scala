import org.apache.spark.sql.functions._

meteo.join(villes,
       meteo.col("id") === villes.col("Id")).
       select("ville","annee","mois_jour",
               "temperature","precipitations").
       show(10)

meteo.join(villes,
       meteo.col("id").eqNullSafe(villes.col("Id"))).
       select("ville","annee","mois_jour",
               "temperature","precipitations").
       show(10)

meteo.join(villes,
       meteo.col("id").equalTo(villes.col("Id"))).
       select("ville","annee","mois_jour",
               "temperature","precipitations").
       show(10)

meteo.join(villes.withColumnRenamed("Id", "id"),"id").
      select("ville","annee","mois_jour",
              "temperature","precipitations").
      show(10)


dfa.show()
dfb.show()

dfa.join(dfb,"ville").show()
dfa.join(dfb,dfa.col("ville") === dfb.col("ville"),"inner").show()
dfa.join(dfb,dfa.col("ville") === dfb.col("ville"),"outer").show()

dfa.join(dfb,dfa.col("ville") === dfb.col("ville"),"left").show()
dfa.join(dfb,dfa.col("ville") === dfb.col("ville"),"right").show()

dfa.join(dfb,dfa.col("ville") === dfb.col("ville"),"full").show()


dfa.join(dfb,dfa.col("ville") === dfb.col("ville"),"left_semi").show()
dfa.join(dfb,dfa.col("ville") === dfb.col("ville"),"left_anti").show()
