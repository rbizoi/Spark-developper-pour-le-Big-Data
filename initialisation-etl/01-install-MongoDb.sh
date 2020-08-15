


echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list

sudo apt-get update
sudo apt-get install -y mongodb-org
echo "mongodb-org hold" | sudo dpkg --set-selections
echo "mongodb-org-server hold" | sudo dpkg --set-selections
echo "mongodb-org-shell hold" | sudo dpkg --set-selections
echo "mongodb-org-mongos hold" | sudo dpkg --set-selections
echo "mongodb-org-tools hold" | sudo dpkg --set-selections


ps --no-headers -o comm 1
sudo systemctl start mongod
sudo systemctl daemon-reload
sudo systemctl status mongod
sudo systemctl enable mongod
sudo systemctl stop mongod
sudo systemctl restart mongod




mongo --host 192.168.99.100 --port 32769
#Pour se placer dans une base:
use <nombase>
#Une base est constituée d’un ensemble de collections, l’équivalent d’une table en relationnel. Pour créer une collection:
db.createCollection("movies")
#La liste des collections est obtenue par:
show collections
#Pour insérer un document JSON dans une collection (ici, movies):
db.movies.insert ({"nom": "nfe024"})
#Il existe donc un objet (javascript) implicite, db, auquel on soumet des demandes d’exécution de certaines méthodes.
#Pour afficher le contenu d’une collection:

db.movies.find()

#C’est un premier exemple d’une fonction de recherche avec MongoDB. On obtient des objets (javascript, encodés en JSON)

{ "_id" : ObjectId("5422d9095ae45806a0e66474"), "nom" : "nfe024" }

#MongoDB associe un identifiant unique à chaque document, de nom conventionnel _id, et lui attribue une valeur si elle n’est pas indiquée explicitement.
#Pour insérer un autre document:

db.movies.insert ({"produit": "Grulband", prix: 230, enStock: true})

#Vous remarquerez que la structure de ce document n’a rien à voir avec le précédent: il n’y a pas de schéma (et donc pas de contrainte) dans MongoDB. On est libre de tout faire (et même de faire n’importe quoi). Nous sommes partis pour mettre n’importe quel objet dans notre collection movies, ce qui revient à reporter les problèmes (contrôles, contraintes, tests sur la structure) vers l’application.
#On peut affecter un identifiant explicitement:
db.movies.insert ({_id: "1", "produit": "Kramölk", prix: 10, enStock: true})

#On peut compter le nombre de documents dans la collection:

db.movies.count()

#Et finalement, on peut supprimer une collection:

db.movies.drop()
