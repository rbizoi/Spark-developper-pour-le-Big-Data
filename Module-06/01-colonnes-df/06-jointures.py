from pyspark.sql.functions import *


meteo.\
     .select('annee','mois_jour','temperature','precipitations')\
     .describe().show()

villes.show()



meteo.join(villes,
       meteo.id == villes.Id)\
       .select('ville','annee','mois_jour',
               'temperature','precipitations')\
       .show(10)

meteo.join(villes,
       meteo['id'].eqNullSafe(villes['Id']))\
       .select('ville','annee','mois_jour',
               'temperature','precipitations')\
       .show(10)

meteo.join(villes.withColumnRenamed('Id', 'id'),'id')\
       .select('ville','annee','mois_jour',
               'temperature','precipitations')\
       .show(10)
