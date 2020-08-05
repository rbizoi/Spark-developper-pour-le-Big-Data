#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

export SPARK_HOME=/usr/share/spark

cat <<FIN_FICHIER > $SPARK_HOME/conf/hive-env.sh
export HADOOP_HOME=/usr/share/hadoop
export HIVE_CONF_DIR=/usr/share/spark/conf
FIN_FICHIER

bash <(curl -s https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/03-Hive/03-hive_site_xml.sh)

wget https://cdn.mysql.com//Downloads/Connector-J/mysql-connector-java-8.0.20.tar.gz
tar -xvzf mysql-connector-java-8.0.20.tar.gz
cp mysql-connector-java-8.0.20/mysql-connector-java-8.0.20.jar $SPARK_HOME/jars
chown -R spark:hadoop $SPARK_HOME
rm mysql-connector-java-8.0.20.tar.gz
rm -Rf mysql-connector-java-8.0.20
