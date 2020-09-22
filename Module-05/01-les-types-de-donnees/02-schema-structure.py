from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

from pyspark.sql.functions import avg,round


import json
import urllib.request
url = url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=occupation-parkings-temps-reel"
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())
lignes = results['nhits']
url = url+"&q=&lang=fr&rows="+str(lignes)
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())['records']

donnees = spark.read.json(sc.parallelize([x['fields'] for x in results]))
donnees.printSchema()
donnees.select('ident','nom_parking','etat_descriptif',
               'libre', 'total').orderBy('libre',ascending=0).show(5)
donnees.columns
schema ='etat int, etat_descriptif string, ident string, idsurfs string, infousager string, libre int, nom_parking string, total int'
donnees = spark.read.json(sc.parallelize([x['fields'] for x in results]),schema)
donnees.printSchema()
donnees.select('ident','nom_parking','etat_descriptif',
               'libre', 'total').orderBy('libre',ascending=0).show(5)

donnees.select('ident','nom_parking','etat_descriptif',
               'libre', 'total').orderBy('libre',ascending=0).show(24)
