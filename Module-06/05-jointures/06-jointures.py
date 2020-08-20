from pyspark.sql.functions import *

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

dfa.show()
dfb.show()

dfa.join(dfb,'ville').show()
dfa.join(dfb,dfa['ville'] == dfb['ville'],'inner').show()
dfa.join(dfb,dfa['ville'] == dfb['ville'],'outer').show()

dfa.join(dfb,dfa['ville'] == dfb['ville'],'left').show()
dfa.join(dfb,dfa['ville'] == dfb['ville'],'right').show()

dfa.join(dfb,dfa['ville'] == dfb['ville'],'full').show()


dfa.join(dfb,dfa['ville'] == dfb['ville'],'left_semi').show()
dfa.join(dfb,dfa['ville'] == dfb['ville'],'left_anti').show()
