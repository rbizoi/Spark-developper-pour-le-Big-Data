
#data.strasbourg.eu


## Localisation et occupation des stations de vélos en libre-service VELHOP
>> `url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=stations-velhop"`<br>
## Parkings sur le territoire de l'Eurométropole
>> `url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=occupation-parkings-temps-reel"`<br>
## Lieux ouverts au public de la catégorie "Sport" - Eurométropole de Strasbourg
>> `url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=lieux_sport"`<br>


>>  `import json                                                                        `<br>
>>  `import urllib.request                                                              `<br>
>>  `url = "https://data.strasbourg.eu/api/records/1.0/search/?dataset=stations-velhop" `<br>
>>  `response = urllib.request.urlopen(url)                                             `<br>
>>  `results = json.loads(response.read().decode())                                     `<br>
>>  `lignes = results['nhits']                                                          `<br>
>>  `url = url+"&q=&lang=fr&rows="+str(lignes)                                          `<br>
>>  `response = urllib.request.urlopen(url)                                             `<br>
>>  `results = json.loads(response.read().decode())['records']                          `<br>
>>  `print(lignes, [x['fields'] for x in results])                                      `<br>
