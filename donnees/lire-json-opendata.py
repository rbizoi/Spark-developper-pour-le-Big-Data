#! /usr/bin/python



url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=stations-velhop"
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=occupation-parkings-temps-reel"
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=lieux_sport"


import json
import urllib.request
url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=stations-velhop"
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())
lignes = results['nhits']
url = url+"&q=&lang=fr&rows="+str(lignes)
response = urllib.request.urlopen(url)
results = json.loads(response.read().decode())['records']
print(lignes, [x['fields'] for x in results])



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
