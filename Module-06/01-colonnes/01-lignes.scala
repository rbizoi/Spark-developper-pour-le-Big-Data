import org.apache.spark.sql.Row

val uneLigne = Row(1,"Isabelle","BIZOÏ")
val uneAutre = Row(2,"Razvan","BIZOÏ")

val lignes = Seq((2,"Razvan","BIZOÏ"),(1,"Isabelle","BIZOÏ"))
val personnes = lignes.toDF("Id","Prenom","Nom")
personnes.printSchema()
personnes.show()
