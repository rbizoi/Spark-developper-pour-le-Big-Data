#!/bin/bash

if [ $USER != "spark" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: spark"
        exit -1
fi

cd ~
mkdir -p ~/donnees
cd ~/donnees

hdfs dfs -mkdir -p donnees/meteo

for annee in `seq 1996 2020`
do
    for mois in `seq 1 12`
    do
        fichier=`printf "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop.%d%02d.csv.gz" $annee $mois`
        wget $fichier
        `printf "gunzip -d synop.%d%02d.csv.gz" $annee $mois`
        `printf "hdfs dfs -moveFromLocal synop.%d%02d.csv donnees/meteo" $annee $mois`
    done
done

wget https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/postesSynop.csv
hdfs dfs -moveFromLocal postesSynop.csv donnees
wget https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/postesSynop.json
wget https://donneespubliques.meteofrance.fr/client/document/doc_parametres_synop_168.pdf

cd ~
hdfs dfs -ls -R donnees
