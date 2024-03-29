#!/bin/bash

if [ $USER != "hdfs" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: hdfs"
        exit -1
fi

cat <<FIN_FICHIER > $HADOOP_HOME/etc/hadoop/hdfs-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

    <property>
      <name>dfs.blocksize</name>
      <value>134217728</value>
    </property>

    <property>
      <name>dfs.datanode.data.dir</name>
      <value>file:/u01/hadoop/hdfs/datanode</value>
      <final>true</final>
    </property>

    <property>
      <name>dfs.datanode.data.dir.perm</name>
      <value>750</value>
    </property>

    <property>
      <name>dfs.datanode.du.reserved</name>
      <value>5722593792</value>
    </property>

    <property>
      <name>dfs.datanode.failed.volumes.tolerated</name>
      <value>0</value>
      <final>true</final>
    </property>

    <property>
      <name>dfs.datanode.max.transfer.threads</name>
      <value>4096</value>
    </property>

    <property>
      <name>dfs.heartbeat.interval</name>
      <value>3</value>
    </property>

    <property>
      <name>dfs.http.policy</name>
      <value>HTTP_ONLY</value>
    </property>

    <property>
      <name>dfs.journalnode.edits.dir</name>
      <value>file:/u01/hadoop/hdfs/journalnode</value>
    </property>

    <property>
      <name>dfs.journalnode.http-address</name>
      <value>0.0.0.0:8480</value>
    </property>

    <property>
      <name>dfs.journalnode.https-address</name>
      <value>0.0.0.0:8481</value>
    </property>

    <property>
      <name>dfs.namenode.accesstime.precision</name>
      <value>0</value>
    </property>

    <property>
      <name>dfs.namenode.acls.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>dfs.namenode.audit.log.async</name>
      <value>true</value>
    </property>

    <property>
      <name>dfs.namenode.avoid.read.stale.datanode</name>
      <value>true</value>
    </property>

    <property>
      <name>dfs.namenode.avoid.write.stale.datanode</name>
      <value>true</value>
    </property>

    <property>
      <name>dfs.namenode.checkpoint.dir</name>
      <value>file:/u01/hadoop/hdfs/checkpoint</value>
    </property>

    <property>
      <name>dfs.namenode.checkpoint.period</name>
      <value>21600</value>
    </property>

    <property>
      <name>dfs.namenode.checkpoint.txns</name>
      <value>1000000</value>
    </property>

    <property>
      <name>dfs.namenode.fslock.fair</name>
      <value>false</value>
    </property>

    <property>
      <name>dfs.namenode.handler.count</name>
      <value>100</value>
    </property>

    <property>
      <name>dfs.namenode.http-address</name>
      <value>`hostname -f`:9870</value>
      <final>true</final>
    </property>

    <property>
      <name>dfs.namenode.https-address</name>
      <value>`hostname -f`:9871</value>
    </property>

    <property>
      <name>dfs.namenode.name.dir</name>
      <value>file:/u01/hadoop/hdfs/namenode</value>
      <final>true</final>
    </property>

    <property>
      <name>dfs.namenode.name.dir.restore</name>
      <value>true</value>
    </property>

    <property>
      <name>dfs.namenode.posix.acl.inheritance.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>dfs.namenode.rpc-address</name>
      <value>`hostname -f`:8020</value>
    </property>

    <property>
      <name>dfs.namenode.safemode.threshold-pct</name>
      <value>0.999</value>
    </property>

    <property>
      <name>dfs.namenode.secondary.http-address</name>
      <value>`hostname -f`:9868</value>
    </property>

    <property>
      <name>dfs.namenode.stale.datanode.interval</name>
      <value>30000</value>
    </property>

    <property>
      <name>dfs.namenode.startup.delay.block.deletion.sec</name>
      <value>3600</value>
    </property>

    <property>
      <name>dfs.namenode.write.stale.datanode.ratio</name>
      <value>1.0f</value>
    </property>

    <property>
      <name>dfs.permissions.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
      <value>false</value>
    </property>

    <property>
      <name>dfs.permissions.superusergroup</name>
      <value>hdfs</value>
    </property>

    <property>
      <name>dfs.cluster.administrators</name>
      <value>hdfs</value>
    </property>

    <property>
      <name>dfs.replication</name>
      <value>3</value>
    </property>

    <property>
      <name>dfs.replication.max</name>
      <value>50</value>
    </property>

    <property>
      <name>dfs.webhdfs.enabled</name>
      <value>true</value>
      <final>true</final>
    </property>

</configuration>
FIN_FICHIER
