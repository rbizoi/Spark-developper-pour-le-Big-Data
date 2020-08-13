from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

from pyspark.sql.functions import avg,round


import json
import urllib.request
url = "https://api.openaq.org/v1/measurements"
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())['results']
print(results)



import json
import urllib.request
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=lieux_sport"
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())
lignes = results['nhits']
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=lieux_sport&q=&lang=fr&rows="+str(lignes)
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())['records']
print([x['fields'] for x in results])
spark.read.json(sc.parallelize([x['fields'] for x in results])).show(lignes)



import json
import urllib.request
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=stations-velhop&q=&lang=fr&rows=1000&facet=na"
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())['records']['fields']
print([x['fields'] for x in results])
spark.read.json(sc.parallelize([x['fields'] for x in results])).show()
print(results)



import json
import urllib.request
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=stations-velhop"
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())['records']
print([x['fields'] for x in results])
spark.read.json(sc.parallelize([x['fields'] for x in results])).show()


import json
import urllib.request
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=occupation-parkings-temps-reel&q=&rows=200"
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())['records']
print([x['fields'] for x in results])
spark.read.json(sc.parallelize([x['fields'] for x in results])).sortBy('ident').show(200)



customSchemaMeteo = StructType([
            StructField('Id'           , StringType() , True),
            StructField('ville'        , StringType() , True),
            StructField('latitude'     , FloatType()  , True),
            StructField('longitude'    , FloatType()  , True),
            StructField('altitude'     , IntegerType(), True)])

stationsDF00  = spark.read.format('csv')   \
      .option('sep',';')                   \
      .option('mergeSchema', 'true')       \
      .option('header','true')             \
      .option('nullValue','mq')            \
      .schema(customSchemaMeteo)           \
      .load('/user/spark/donnees/postesSynop.csv')   \
      .cache()
stationsDF02.printSchema()
stationsDF01  = spark.read.format('csv') \
      .option('sep',';')                   \
      .option('mergeSchema', 'true')       \
      .option('header','true')             \
      .option('nullValue','mq')            \
      .option('inferSchema', 'true')          \
      .load('/user/spark/donnees/postesSynop.csv')   \
      .toDF('id','ville','latitude','longitude','altitude')\
      .cache()
stationsDF02.printSchema()

customSchemaMeteo = "id STRING, ville STRING, latitude FLOAT, longitude FLOAT, altitude INT"
stationsDF02  = spark.read.format('csv')   \
      .option('sep',';')                   \
      .option('mergeSchema', 'true')       \
      .option('header','true')             \
      .option('nullValue','mq')            \
      .schema(customSchemaMeteo)           \
      .load('/user/spark/donnees/postesSynop.csv')   \
      .cache()
stationsDF02.printSchema()
