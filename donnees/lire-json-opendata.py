#! /usr/bin/python



url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=stations-velhop"
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=occupation-parkings-temps-reel"
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=lieux_sport"
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=vente_a_la_ferme"
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=boucles_sportives_vitaboucle"
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=livraison_de_paniers"
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=lieux_arbres-remarquables"
import json
import urllib.request
#url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=stations-velhop"
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())
lignes = results['nhits']
url = url+"&q=&lang=fr&rows="+str(lignes)
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())['records']
print(lignes, [x['fields'] for x in results])

print(results)



import json
import urllib.request
url = url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=occupation-parkings-temps-reel"
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())
lignes = results['nhits']
url = url+"&q=&lang=fr&rows="+str(lignes)
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())['records']
print(lignes, [x['fields'] for x in results])



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
    print(dataset)



utl = 'https://data.strasbourg.eu/api/records/1.0/search/?dataset='
for dataset in ['boucles_sportives_vitaboucle',
                'indices-de-qualite-de-lair-a-strasbourg',
                'lieux_sport',
                'livraison_de_paniers',
                'marches_ems',
                'occupation-parkings-temps-reel',
                'offre-de-formation-dans-le-numerique',
                'patrimoine_quartier',
                'stationnement-payant',
                'stations-velhop',
                'vente_a_la_ferme']:
    ecrireJsonHDFS(url,dataset)



# https://rapidapi.com/apidojo/api/yahoo-finance1?endpoint=apiendpoint_82fce9c7-f8eb-4fbb-a66e-1b75f7c26d18


import http.client

conn = http.client.HTTPSConnection("apidojo-yahoo-finance-v1.p.rapidapi.com")

headers = {
    'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com",
    'x-rapidapi-key': "5c562940b7msh44c3eb63a28171bp19db68jsnb1a2ffeb6f61"
    }

conn.request("GET", "/market/get-spark?interval=1m&range=1d&symbols=AMZN%252CAAPL%252CWDC%252CREYN%252CAZN%252CYM%253DF", headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))




import requests

url = "https://community-open-weather-map.p.rapidapi.com/weather"

querystring = {"callback":"test","id":"2172797","units":"%22metric%22 or %22imperial%22","mode":"xml%2C html","q":"London%2Cuk"}

headers = {
    'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
    'x-rapidapi-key': "5c562940b7msh44c3eb63a28171bp19db68jsnb1a2ffeb6f61"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)
