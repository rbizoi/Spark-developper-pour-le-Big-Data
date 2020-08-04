#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

REPERTOIRE=/var/run/hadoop
if [ ! -d "$REPERTOIRE" ]; then
    echo "Creation du répértoire : /var/run/hadoop"
    mkdir /var/run/hadoop
    chown -R hdfs:hadoop /var/run/hadoop
fi

REPERTOIRE=/var/run/yarn
if [ ! -d "$REPERTOIRE" ]; then
    echo "Creation du répértoire : /var/run/yarn"
    mkdir /var/run/yarn
    chown -R hdfs:hadoop /var/run/yarn
fi

REPERTOIRE=/var/run/spark
if [ ! -d "$REPERTOIRE" ]; then
    echo "Creation du répértoire : /var/run/spark"
    mkdir /var/run/spark
    chown -R spark:hadoop /var/run/spark
fi

REPERTOIRE=/var/run/zeppelin
if [ ! -d "$REPERTOIRE" ]; then
    echo "Creation du répértoire : /var/run/zeppelin"
    mkdir /var/run/zeppelin
    chown -R zeppelin:hadoop /var/run/zeppelin
fi


su -c /usr/share/hadoop/sbin/start-dfs.sh - hdfs
sleep 60
su -c /usr/share/hadoop/sbin/start-yarn.sh - hdfs
sleep 5
su -c /usr/share/spark/sbin/start-all.sh - spark
sleep 5
systemctl start zeppelin
