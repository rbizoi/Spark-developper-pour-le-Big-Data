#!/bin/bash

if [ $USER != "hdfs" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: hdfs"
        exit -1
fi
