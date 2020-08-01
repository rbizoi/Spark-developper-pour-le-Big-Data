#!/bin/bash

if [ $USER != "hdfs" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: hdfs"
        exit -1
fi


cat <<FIN_FICHIER > $HADOOP_HOME/etc/hadoop/core-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://`hostname -f`:8020</value>
    </property>

    <property>
      <name>fs.trash.interval</name>
      <value>360</value>
    </property>

    <property>
      <name>hadoop.http.authentication.simple.anonymous.allowed</name>
      <value>true</value>
    </property>

    <property>
      <name>hadoop.http.cross-origin.allowed-headers</name>
      <value>X-Requested-With,Content-Type,Accept,Origin,WWW-Authenticate,Accept-Encoding,Transfer-Encoding</value>
    </property>

    <property>
      <name>hadoop.http.cross-origin.allowed-methods</name>
      <value>GET,PUT,POST,OPTIONS,HEAD,DELETE</value>
    </property>

    <property>
      <name>hadoop.http.cross-origin.allowed-origins</name>
      <value>*</value>
    </property>

    <property>
      <name>hadoop.http.cross-origin.max-age</name>
      <value>1800</value>
    </property>

    <property>
      <name>hadoop.http.filter.initializers</name>
      <value>org.apache.hadoop.security.AuthenticationFilterInitializer,org.apache.hadoop.security.HttpCrossOriginFilterInitializer,org.apache.hadoop.http.lib.StaticUserWebFilter</value>
    </property>

    <property>
      <name>hadoop.http.staticuser.user</name>
      <value>hdfs</value>
    </property>

    <property>
      <name>hadoop.security.auth_to_local</name>
      <value>DEFAULT</value>
    </property>

    <property>
      <name>hadoop.security.authentication</name>
      <value>simple</value>
    </property>

    <property>
      <name>hadoop.security.authorization</name>
      <value>false</value>
    </property>

    <property>
      <name>hadoop.security.instrumentation.requires.admin</name>
      <value>false</value>
    </property>

    <property>
      <name>hadoop.security.key.provider.path</name>
      <value></value>
    </property>

    <property>
        <name>io.compression.codecs</name>
        <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.SnappyCodec</value>
    </property>

    <property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
    </property>

    <property>
        <name>io.serializations</name>
        <value>org.apache.hadoop.io.serializer.WritableSerialization</value>
    </property>

    <property>
        <name>ipc.client.connect.max.retries</name>
        <value>50</value>
    </property>

    <property>
        <name>ipc.client.connection.maxidletime</name>
        <value>30000</value>
    </property>

    <property>
        <name>ipc.client.idlethreshold</name>
        <value>8000</value>
    </property>

    <property>
        <name>ipc.server.tcpnodelay</name>
        <value>true</value>
    </property>

    <property>
        <name>mapreduce.jobtracker.webinterface.trusted</name>
        <value>false</value>
    </property>


    <property>
        <name>hadoop.http.cross-origin.enabled</name>
        <value>false</value>
    </property>

    <property>
        <name>hadoop.http.cross-origin.allowed-origins</name>
        <value>*</value>
    </property>

    <property>
        <name>hadoop.http.cross-origin.allowed-methods</name>
        <value>GET,POST,HEAD</value>
    </property>

    <property>
        <name>hadoop.http.cross-origin.allowed-headers</name>
        <value>X-Requested-With,Content-Type,Accept,Origin</value>
    </property>

    <property>
        <name>hadoop.http.cross-origin.max-age</name>
        <value>1800</value>
    </property>

</configuration>
FIN_FICHIER
