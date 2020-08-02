#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

systemctl status mysql

sed -i -e "s/127.0.0.1/0.0.0.0/g" /etc/mysql/mysql.conf.d/mysqld.cnf
cat /etc/mysql/mysql.conf.d/mysqld.cnf  | grep bind-address

systemctl restart mysql

mot_de_passe=CoursSPARK#






cat << FIN_FICHIER > create-metastore.mysql.sql
ALTER USER root@localhost IDENTIFIED BY 'CoursSPARK#';

DROP USER IF EXISTS 'spark'@'%';
DROP USER IF EXISTS 'spark'@localhost;
DROP DATABASE IF EXISTS metastore;

CREATE DATABASE metastore;

CREATE USER 'spark'@'localhost' IDENTIFIED BY 'CoursSPARK3#20';
CREATE USER 'spark'@'%' IDENTIFIED BY 'CoursSPARK3#20';

REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'spark'@'localhost';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'spark'@'%';

GRANT ALL PRIVILEGES ON metastore.* TO 'spark'@'localhost';
GRANT ALL PRIVILEGES ON metastore.* TO 'spark'@'%';

FLUSH PRIVILEGES;

USE metastore;

SOURCE $HIVE_HOME/scripts/metastore/upgrade/mysql/hive-schema-2.3.0.mysql.sql

FIN_FICHIER

mysql --user=spark --password=CoursSPARK# < create-metastore.mysql.sql > create-metastore.mysql.txt

cat create-metastore.mysql.txt

cat << FIN_FICHIER > verifie-metastore.mysql.sql
USE metastore;

SHOW DATABASES;

SELECT schema_name
FROM information_schema.schemata
WHERE schema_name = 'metastore';

--select Host,
--        User,
--        Select_priv,
--        Insert_priv ,
--        Update_priv ,
--        Delete_priv ,
--        Create_priv ,
--        Drop_priv
--from mysql.user;


SHOW TABLES FROM metastore ;
SHOW GRANTS FOR spark;

FIN_FICHIER


mysql --user=spark --password=CoursSPARK# < verifie-metastore.mysql.sql > verifie-metastore.mysql.txt


cat verifie-metastore.mysql.txt
