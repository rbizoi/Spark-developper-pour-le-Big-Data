#!/bin/bash

if [ $USER != "spark" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: spark"
        exit -1
fi

hdfs dfs -mkdir /spark-jars
hdfs dfs -mkdir /spark-history
hdfs dfs -put $SPARK_HOME/jars/* /spark-jars
hdfs dfs -ls /spark-jars
hdfs dfs -mkdir /user/spark/entrepot-de-donnees

$SPARK_HOME/sbin/start-all.sh
jps
