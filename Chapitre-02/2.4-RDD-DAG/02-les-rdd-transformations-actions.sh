donnees00 = spark.sparkContext.\
       textFile('/user/spark/donnees/postesSynop.csv').\
       persist()
donnees00
donnees01 = donnees00.filter( lambda ligne :
                         str(ligne)[0].isdigit() )
donnees02 = donnees01.map( lambda ligne :
                          ligne.split(";"))
donnees00.id()
donnees01.id()
donnees02.id()
donnees02.take(5)
