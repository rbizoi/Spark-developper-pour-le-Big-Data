#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

su -c /usr/share/hadoop/sbin/stop-yarn.sh - hdfs
sleep 5
su -c /usr/share/hadoop/sbin/stop-dfs.sh - hdfs
sleep 5
su -c /usr/share/spark/sbin/stop-all.sh - spark
