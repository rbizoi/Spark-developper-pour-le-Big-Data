
import json
import urllib.request

url = 'https://data.strasbourg.eu/api/records/1.0/search/?dataset=occupation-parkings-temps-reel&q=&lang=fr&rows=28'
donnees = spark.read.json(sc.parallelize(
            [x['fields'] for x in
                  json.loads(urllib.request.urlopen(url
                         ).read().decode())['records']
                ]))
donnees.select ('nom_parking','libre','total').show(3)

fichier = '/user/spark/donnees/json/parking_stras_json'
format  = 'json'
donnees.write.mode('overwrite').format(format).save(fichier)
