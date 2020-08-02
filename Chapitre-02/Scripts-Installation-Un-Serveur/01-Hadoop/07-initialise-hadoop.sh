#!/bin/bash

if [ $USER != "hdfs" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: hdfs"
        exit -1
fi

hdfs namenode -format
start-dfs.sh
jps
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/hdfs
hdfs dfs -mkdir /user/spark
hdfs dfs -mkdir /user/hive
hdfs dfs -ls -R /
jps
start-yarn.sh
jps
hdfs dfsadmin -report
