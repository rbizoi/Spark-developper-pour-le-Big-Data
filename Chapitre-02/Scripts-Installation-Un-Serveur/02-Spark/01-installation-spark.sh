#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

wget https://downloads.apache.org/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz
tar xzvf spark-3.0.0-bin-hadoop3.2.tgz
rm -f spark-3.0.0-bin-hadoop3.2.tgz
mv spark-3.0.0-bin-hadoop3.2 spark
mv spark /usr/share

cat << FIN_FICHIER > /etc/profile.d/spark.sh
#!/bin/bash
export SPARK_HOME=/usr/share/spark
export PYSPARK_PYTHON=/bin/python3
#export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export PATH=\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$PATH
FIN_FICHIER

export SPARK_HOME=/usr/share/spark

cat <<FIN_FICHIER > $SPARK_HOME/conf/spark-env.sh
#!/usr/bin/env bash
SPARK_EXECUTOR_MEMORY="2G"
SPARK_DRIVER_MEMORY="1024M"
export SPARK_DAEMON_MEMORY=1024m
USER="\$(whoami)"
SPARK_IDENT_STRING=\$USER
SPARK_NICENESS=0
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8082
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_LOG_DIR=/var/log/spark/\$USER
export SPARK_PID_DIR=/var/run/spark/\$USER
FIN_FICHIER

cat <<FIN_FICHIER > $SPARK_HOME/conf/spark-defaults.conf
spark.master yarn
spark.eventLog.dir hdfs://`hostname -f`:8020/spark-history/
spark.eventLog.enabled true
FIN_FICHIER

cat <<FIN_FICHIER > $SPARK_HOME/conf/slaves
jupiter.olimp.fr
FIN_FICHIER

sudo chown -R spark:hadoop /usr/share/spark
ls -al /usr/share/spark

sudo rm -Rf /var/*/spark
sudo mkdir /var/log/spark
sudo mkdir /var/run/spark
sudo chown -R spark:hadoop /var/*/spark
ls -al /var/*/spark


su - spark

hdfs dfs -mkdir /spark-jars
hdfs dfs -put $SPARK_HOME/jars/* /spark-jars
hdfs dfs -ls /spark-jars
