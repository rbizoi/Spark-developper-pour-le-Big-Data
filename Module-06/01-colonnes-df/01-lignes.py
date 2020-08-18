from pyspark.sql import Row

uneLigne = Row(1,"Isabelle","BIZOÏ")
uneAutre = Row(2,"Razvan","BIZOÏ")
listeLignes = [uneLigne,uneAutre]
personnes = spark.createDataFrame(listeLignes,["Id","Prenom","Nom"])
personnes.show()
