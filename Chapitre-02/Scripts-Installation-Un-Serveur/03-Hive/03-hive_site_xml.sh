#!/bin/bash

cat <<FIN_FICHIER > $HIVE_HOME/conf/hive-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://jupiter.olimp.fr:3306/metastore?serverTimezone=UTC</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.cj.jdbc.Driver</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>spark</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>CoursSPARK#</value>
  </property>

  <property>
    <name>datanucleus.autoCreateSchema</name>
    <value>false</value>
  </property>

  <property>
    <name>datanucleus.cache.level2.type</name>
    <value>none</value>
  </property>

  <property>
    <name>datanucleus.fixedDatastore</name>
    <value>false</value>
  </property>

  <property>
    <name>hive.metastore.schema.verification</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.metastore.client.connect.retry.delay</name>
    <value>5s</value>
  </property>

  <property>
    <name>hive.metastore.client.socket.timeout</name>
    <value>1800s</value>
  </property>

  <property>
    <name>hive.metastore.connect.retries</name>
    <value>24</value>
  </property>

  <property>
    <name>hive.metastore.db.type</name>
    <value>MYSQL</value>
  </property>

  <property>
    <name>hive.metastore.dml.events</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.metastore.execute.setugi</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.metastore.failure.retries</name>
    <value>24</value>
  </property>

  <property>
    <name>metastore.create.as.acid</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.server2.zookeeper.namespace</name>
    <value>hiveserver2</value>
  </property>

  <property>
    <name>hive.zookeeper.client.port</name>
    <value>2181</value>
  </property>

  <property>
    <name>hive.zookeeper.namespace</name>
    <value>hive_zookeeper_namespace</value>
  </property>

  <property>
    <name>hive.zookeeper.quorum</name>
    <value>`hostname -f`:2181</value>
  </property>

  <property>
    <name>hive.execution.engine</name>
    <value>spark</value>
  </property>

  <property>
      <name>spark.master</name>
      <value>spark://`hostname -f`:7077</value>
  </property>

  <property>
      <name>spark.eventLog.enabled</name>
      <value>true</value>
  </property>

  <property>
      <name>spark.eventLog.dir</name>
      <value>/tmp</value>
  </property>

  <property>
      <name>spark.serializer</name>
      <value>org.apache.spark.serializer.KryoSerializer</value>
  </property>

  <property>
    <name>spark.yarn.jars</name>
    <value>hdfs:///spark-jars/*</value>
  </property>

  <property>
    <name>hive.vectorized.execution.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.vectorized.execution.mapjoin.minmax.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.vectorized.execution.mapjoin.native.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.vectorized.execution.reduce.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>hive.vectorized.groupby.checkinterval</name>
    <value>4096</value>
  </property>

  <property>
    <name>hive.vectorized.groupby.flush.percent</name>
    <value>0.1</value>
  </property>

  <property>
    <name>hive.vectorized.groupby.maxentries</name>
    <value>100000</value>
  </property>

  <property>
    <name>hive.exec.scratchdir</name>
    <value>/tmp/spark</value>
  </property>

  <property>
    <name>hive.metastore.client.connect.retry.delay</name>
    <value>5</value>
  </property>

  <property>
    <name>hive.metastore.client.socket.timeout</name>
    <value>1800</value>
  </property>

  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>

  <property>
    <name>hive.server2.thrift.http.port</name>
    <value>10001</value>
  </property>

  <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
  </property>

  <property>
    <name>hive.server2.transport.mode</name>
    <value>binary</value>
  </property>

  <property>
    <name>metastore.catalog.default</name>
    <value>spark</value>
  </property>

</configuration>
FIN_FICHIER
