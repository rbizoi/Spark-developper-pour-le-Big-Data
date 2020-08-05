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



https://websiteforstudents.com/how-to-install-apache-cassandra-on-ubuntu-20-04-18-04/


wget -q -O - https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -
sudo sh -c 'echo "deb http://www.apache.org/dist/cassandra/debian 311x main" > /etc/apt/sources.list.d/cassandra.sources.list'
sudo apt update
sudo apt install cassandra

nodetool status
cqlsh


https://www.osradar.com/install-apache-cassandra-ubuntu-20-04/

sudo apt update
sudo apt upgrade
sudo apt install wget curl gnupg gnupg1 gnupg2
wget https://www.apache.org/dist/cassandra/KEYS
sudo apt-key add KEYS
echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
echo "deb http://www.apache.org/dist/cassandra/debian 40x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
sudo apt update
sudo apt install cassandra
sudo systemctl status cassandra

/etc/cassandra/cassandra.yaml
cluster_name : ['cluster_name']
seeds: ["ip_address", "ip_address"]
storage_port :[port]


sudo systemctl restart cassandra




https://www.howtoforge.com/how-to-install-and-use-mongodb-on-ubuntu-2004/

apt-get install gnupg -y
wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.2.list
apt-get update -y
apt-get install mongodb-org -y
systemctl start mongod
systemctl enable mongod
systemctl status mongod
