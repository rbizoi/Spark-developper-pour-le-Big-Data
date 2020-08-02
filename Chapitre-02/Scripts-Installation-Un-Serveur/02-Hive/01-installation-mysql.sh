#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

mysql_secure_installation <<FIN_FICHIER

y
CoursSPARK#
CoursSPARK#
y
y
y
y
FIN_FICHIER

sed -i -e "s/127.0.0.1/0.0.0.0/g" /etc/mysql/mysql.conf.d/mysqld.cnf
cat /etc/mysql/mysql.conf.d/mysqld.cnf  |grep bind-address

systemctl restart mysql
