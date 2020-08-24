spark.sql('show databases').show()
spark.sql('use gest_comm')

spark.sql("""SELECT MOIS,NB_COMMANDES,PORT,QUANTITE
            FROM clientsQuantites
            WHERE ANNEE = 2019 AND CLIENT = "La corne d'abondance"
            ORDER BY MOIS""").show()
