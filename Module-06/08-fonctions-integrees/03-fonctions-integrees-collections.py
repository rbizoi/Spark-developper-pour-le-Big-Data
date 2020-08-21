from pyspark.sql.functions import *
import json

url = 'https://data.straliasbourg.eu/api/records/1.0/search/?dataliaset=vente_a_la_ferme&q=&lang=fr&rows=39'

def lireUnJsonURL(url):
    chaine = urllib.request.urlopen(url)
    jsonList = [json.loads(chaine.read().decode())]
    jsonDf = spark.read.json(sc.parallelize(jsonList))
    return jsonDf

donnees = lireUnJsonURL(url)
donnees.printSchema()

donnees.select(  'nhits','parameters',
                 donnees['records'].getField('fields').alias('fields'),
                 donnees['records'].getField('geometry').alias('geometry'),
               ).show()

donnees.select( donnees['records'].getField('fields').getField('nom').alias('nom'),
                 donnees['records'].getField('fields').getField('commune').alias('commune'),
                 donnees['records'].getField('fields').getField('code_postal').alias('code_postal')
               ).show()

donnees.show()
donnees.select('nhits','parameters',
                explode(donnees['records'].getField('fields'))\
                .alias('fields')).show()

fermes = donnees.select( explode('records').alias('records'))\
        .select('records.*')\
        .select('recordid','fields.*','geometry','record_timestamp')
fermes.printSchema()
fermes.select('nom','adresse','code_postal','famille').show()

fermes.select("nom","adresse","code_postal","commune","famille").show()

fermes.to_json()

donnees.select('parameters').printSchema()
donnees.select('nhits','parameters').show()
donnees.select('nhits','parameters.*').show()
