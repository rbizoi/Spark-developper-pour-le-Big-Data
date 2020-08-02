#!/bin/bash

if [ $USER != "spark" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: spark"
        exit -1
fi

cd ~
mkdir -p ~/donnees/meteo
cd ~/donnees/meteo

for annee in `seq 1996 2020`
do
    for mois in `seq 1 12`
    do
        fichier=`printf "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop.%d%02d.csv.gz" $annee $mois`
        wget $fichier
        `printf "gunzip -d synop.%d%02d.csv.gz" $annee $mois`
    done
done

cd ~/donnees

wget https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/postesSynop.csv
wget https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/postesSynop.json
wget https://donneespubliques.meteofrance.fr/client/document/doc_parametres_synop_168.pdf
