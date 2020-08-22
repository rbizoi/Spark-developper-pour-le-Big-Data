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

meteoMM.agg(sum("precipitations")).show()




















meteoMM.select('jour,'precipitations,sum("precipitations").over().as("somme totale")).show()


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
