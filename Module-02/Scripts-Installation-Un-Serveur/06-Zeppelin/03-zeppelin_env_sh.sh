#!/bin/bash

cat <<FIN_FICHIER > $ZEPPELIN_HOME/conf/zeppelin-env.sh
export MASTER=spark://`hostname -f`:7077
export SPARK_YARN_JAR=hdfs:///spark-jars
export ZEPPELIN_LOG_DIR=/var/log/zeppelin
export ZEPPELIN_PID_DIR=/var/run/zeppelin/zeppelin
export HADOOP_CONF_DIR=/usr/share/hadoop/etc/hadoop
FIN_FICHIER
