#!/bin/bash

if [ $USER != "hadoop" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: hadoop"
        exit -1
fi

cat <<FIN_FICHIER > $HADOOP_HOME/etc/hadoop/mapred-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
       <property>
           <name>mapreduce.framework.name</name>
           <value>yarn</value>
       </property>

       <property>
         <name>mapreduce.cluster.administrators</name>
         <value>hdfs</value>
       </property>

       <property>
         <name>mapreduce.task.io.sort.mb</name>
         <value>100</value>
       </property>

       <property>
         <name>mapreduce.cluster.acls.enabled</name>
         <value>false</value>
       </property>

       <property>
          <name>yarn.app.mapreduce.am.env</name>
          <value>HADOOP_MAPRED_HOME=/usr/share/hadoop</value>
       </property>

       <property>
          <name>mapreduce.map.env</name>
          <value>HADOOP_MAPRED_HOME=/usr/share/hadoop</value>
       </property>

       <property>
          <name>mapreduce.reduce.env</name>
          <value>HADOOP_MAPRED_HOME=/usr/share/hadoop</value>
       </property>

       <property>
         <name>mapreduce.reduce.java.opts</name>
         <value>-Xmx819m</value>
       </property>

       <property>
         <name>mapreduce.reduce.log.level</name>
         <value>INFO</value>
       </property>

       <property>
         <name>mapreduce.reduce.memory.mb</name>
         <value>1024</value>
       </property>

       <property>
         <name>mapreduce.reduce.shuffle.fetch.retry.enabled</name>
         <value>1</value>
       </property>

       <property>
         <name>mapreduce.reduce.shuffle.fetch.retry.interval-ms</name>
         <value>1000</value>
       </property>

       <property>
         <name>mapreduce.reduce.shuffle.fetch.retry.timeout-ms</name>
         <value>30000</value>
       </property>

       <property>
         <name>mapreduce.reduce.shuffle.input.buffer.percent</name>
         <value>0.7</value>
       </property>

       <property>
         <name>mapreduce.reduce.shuffle.merge.percent</name>
         <value>0.66</value>
       </property>

       <property>
         <name>mapreduce.reduce.shuffle.parallelcopies</name>
         <value>30</value>
       </property>
</configuration>
FIN_FICHIER
