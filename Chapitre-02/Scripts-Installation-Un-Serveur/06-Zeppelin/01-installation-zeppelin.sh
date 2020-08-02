#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

wget wget https://mega.nz/file/OOhBjZBJ#8Ocuwx7u67I_ci74pnKzaHwaBUtUeispYxT4jzpDGec
tar xzvf kafka_2.12-2.5.0.tgz
rm -f kafka_2.12-2.5.0.tgz
mv kafka_2.12-2.5.0 kafka
mv kafka /usr/share
