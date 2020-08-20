from pyspark.sql.functions import *

dfa.union(dfb).orderBy('ville').show(dfa.count()+dfb.count())
dfa.select('ville').union(dfb.select('ville')).distinct().orderBy('ville').show(dfa.count()+dfb.count())


meteo.select('id','temperature')\
     .union(villes.select('ville','altitude'))\
     .orderBy('id').show(5)

meteo.select('id','temperature')\
     .union(villes.select('ville','altitude'))\
     .orderBy(desc('id')).show(5)

dfa.select('ville').intersect(dfb.select('ville')).show()
