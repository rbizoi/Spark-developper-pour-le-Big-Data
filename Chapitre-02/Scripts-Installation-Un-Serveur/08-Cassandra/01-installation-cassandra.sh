#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

wget https://downloads.apache.org/cassandra/4.0-beta1/apache-cassandra-4.0-beta1-bin.tar.gz
tar xzvf apache-cassandra-4.0-beta1-bin.tar.gz
rm -f apache-cassandra-4.0-beta1-bin.tar.gz
mv apache-cassandra-4.0-beta1 cassandra
mv cassandra /usr/share
