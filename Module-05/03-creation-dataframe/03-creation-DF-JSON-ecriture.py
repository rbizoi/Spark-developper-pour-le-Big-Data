
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


url = 'https://data.strasbourg.eu/api/records/1.0/search/?dataset='
dataset = 'livraison_de_paniers'
def ecrireJsonHDFS(url,dataset):
    urlDS = url+dataset
    urlDS = urlDS+"&q=&lang=fr&rows="+str(json.loads(urllib.request.urlopen(urlDS).read().decode())['nhits'])
    donnees = spark.read.json(sc.parallelize(
                [x['fields'] for x in
                      json.loads(urllib.request.urlopen(urlDS
                             ).read().decode())['records']
                    ]))
    fichier = '/user/spark/donnees/json/'+dataset
    format  = 'json'
    donnees.write.mode('overwrite').format(format).save(fichier)
