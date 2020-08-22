import org.apache.spark.sql.functions._

val meteoFance = meteo.where("id < 8000").
             join( villes.withColumnRenamed("Id", "id"),"id").
             select(initcap(regexp_replace('ville,"-"," ")).alias("ville"),
                     'annee,'mois,'jour,'temperature,
                     'humidite,'visibilite,'pression,'precipitations)

meteoFance.where("ville = 'Mont De Marsan' and "+
                 "annee = 2019 and mois = 11 and jour < 11").
           selectExpr("ville","annee","mois","jour","temperature as t",
                      "humidite as h","visibilite as v",
                      "pression as p","precipitations as e").
           show()

val meteoMM = meteoFance.where("ville = 'Mont De Marsan' and "+
                                "annee = 2019 and "+
                                "mois = 11 and jour < 11").
                         select("jour","temperature",
                                "humidite","visibilite",
                                "pression","precipitations")
meteoMM.show()
meteoMM.groupBy("jour").
        agg( round(sum("precipitations"),2).alias("precipitations")).
        orderBy(col("jour")).
        rollup("jour").
        agg( round(sum("precipitations"),2).alias("precipitations")).
        orderBy(col("jour").asc_nulls_last).
        show()

val meteoMM = meteoFance.where("ville = 'Mont De Marsan'").
           select("ville","annee","mois",
                           "jour","temperature","precipitations").
           groupBy("ville", "annee", "mois", "jour").
           agg( round(avg("temperature"),2).alias("temperature"),
                round(sum("precipitations"),2).alias("precipitations")).
           select("annee","mois","jour","temperature","precipitations")
meteoMM.show(5)

import org.apache.spark.sql.expressions.Window

val jour          = Window.partitionBy("jour")
val mois          = Window.partitionBy("mois")
val moisAnnee     = Window.partitionBy("mois","annee")
val annee         = Window.partitionBy("annee")
val jourMois      = Window.partitionBy("jour","mois")
val jourMoisAnnee = Window.partitionBy("jour","mois","annee")

meteoMM.select('jour,'mois,'annee,'precipitations.as("prec"),
          round(sum("precipitations").over(jourMoisAnnee),2).as("s1"),
          round(sum("precipitations").over(jourMois),2).as("s2"),
          round(sum("precipitations").over(moisAnnee),2).as("s3"),
          round(sum("precipitations").over(mois),2).as("s4"),
          round(sum("precipitations").over(annee),2).as("s5"),
          round(sum("precipitations").over(jour),2).as("s6")).
        show(28)

meteoMM.where("annee = 1996 and mois = 12 and jour = 1").agg(sum("precipitations").as("s1")).show()
meteoMM.where("mois = 12 and jour = 1").agg(sum("precipitations").as("s2")).show()
meteoMM.where("annee = 1996 and mois = 12").agg(sum("precipitations").as("s3")).show()
meteoMM.where("mois  = 12"  ).agg(sum("precipitations").as("s4")).show()
meteoMM.where("annee = 1996").agg(sum("precipitations").as("s5")).show()
meteoMM.where("jour = 1").agg(sum("precipitations").as("s6")).show()
meteoMM.agg(sum("precipitations")).show()

val jour    = Window.partitionBy("mois")
val jourOby = Window.partitionBy("mois").orderBy("jour")

meteoMM.where("annee = 2019").
        select('mois,'jour,
          col("temperature").alias("temp"),
          round(avg("temperature").over(jourOby),2).alias("s1"),
          round(avg("temperature").over(jour),2).alias("s2"),
          col("precipitations").alias("prec"),
          round(sum("precipitations").over(jourOby),2).alias("s3"),
          round(sum("precipitations").over(jour),2).alias("s4")).
        show(32)

val jourPOby = Window.orderBy("mois","jour")
val jourOby  = Window.orderBy("jour")

meteoMM.where("annee = 2019").
        select('mois,'jour,'precipitations.as("prec"),
          round(sum("precipitations").over(jourPOby),2).as("s1"),
          round(sum("precipitations").over(jourOby),2).as("s2")).
        orderBy("jour","mois").
        show(14)

val jour = Window.orderBy("mois").rowsBetween(-1, 1)
meteoMM.where("annee = 2019").
       groupBy("mois").
       agg( round(avg("temperature"),2).alias("temp"),
             round(sum("precipitations"),2).alias("prec")).
       select('mois,'temp,
              round(avg("temp").over(jour),2).alias("s1"),
              'prec,
               round(sum("prec").over(jour),2).alias("s2")).
       show(32)






meteoMM.where("annee = 1996 and mois = 12 and jour = 1").agg(sum("precipitations")).show()






df.select(
  sum("price").over(),
  avg("price").over()
)

window = Window.partitionBy()




meteoFance.where("ville = 'Strasbourg Entzheim' and "+
                "annee = 2020 and mois = 7 and jour < 4").
           orderBy("jour").
           rollup("jour").
           agg(round(sum("precipitations"),2).alias("precipitations")).
           orderBy(col("jour")).
           show()

val meteoFance01 = meteoFance.where("ville = 'Strasbourg Entzheim' and "+
                                    "annee = 2020 and mois = 7 and jour < 4").
                       cache()

meteoFance01.show(200)

meteoFance01.orderBy("jour").
             rollup("jour").
             agg(round(sum("temperature"),2).alias("temperature")).
             orderBy(col("jour")).
             show()




    agg( grouping("id").alias("r_id*2^2"),
         grouping("annee").alias("r_annee*2^1"),
         grouping("mois").alias("r_mois*2^0"),
         (grouping("id")*4+grouping("annee")*2+grouping("mois")).alias("r_perso"),
         grouping_id().alias("regroupement")
       ).
    orderBy(col("id"),col("annee"),col("mois")).
    where('regroupement > 0).
