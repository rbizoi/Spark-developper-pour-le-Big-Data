#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

systemctl status mysql

sed -i -e "s/127.0.0.1/0.0.0.0/g" /etc/mysql/mysql.conf.d/mysqld.cnf
cat /etc/mysql/mysql.conf.d/mysqld.cnf  | grep bind-address

systemctl restart mysql

wget https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/02-Hive/hive-schema-2.3.0.mysql.sql
wget https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/02-Hive/hive-txn-schema-2.3.0.mysql.sql

cat << FIN_FICHIER > create-metastore.mysql.sql
ALTER USER root@localhost IDENTIFIED BY '[CoursSPARK#]';
DROP USER IF EXISTS 'spark'@'%';
DROP USER IF EXISTS 'spark'@localhost;
DROP DATABASE IF EXISTS metastore;
CREATE DATABASE metastore;
CREATE USER 'spark'@'localhost' IDENTIFIED BY '[CoursSPARK#]';
CREATE USER 'spark'@'%' IDENTIFIED BY '[CoursSPARK#]';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'spark'@'localhost';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'spark'@'%';
GRANT ALL PRIVILEGES ON metastore.* TO 'spark'@'localhost';
GRANT ALL PRIVILEGES ON metastore.* TO 'spark'@'%';
FLUSH PRIVILEGES;
USE metastore;
SOURCE ~/hive-schema-2.3.0.mysql.sql
FIN_FICHIER

mysql --user=root --password='[CoursSPARK#]' < create-metastore.mysql.sql > create-metastore.mysql.txt

cat create-metastore.mysql.txt

cat << FIN_FICHIER > verifie-metastore.mysql.sql
USE metastore;

SHOW DATABASES;

SELECT schema_name
FROM information_schema.schemata
WHERE schema_name = 'metastore';

select Host,
        User,
        Select_priv,
        Insert_priv ,
        Update_priv ,
        Delete_priv ,
        Create_priv ,
        Drop_priv
from mysql.user;

SHOW TABLES FROM metastore ;
SHOW GRANTS FOR spark;

FIN_FICHIER

mysql --user=spark --password="CoursSPARK#" --database=metastore < verifie-metastore.mysql.sql > verifie-metastore.mysql.txt

cat verifie-metastore.mysql.txt
