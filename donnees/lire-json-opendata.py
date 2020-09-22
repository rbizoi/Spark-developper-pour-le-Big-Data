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
