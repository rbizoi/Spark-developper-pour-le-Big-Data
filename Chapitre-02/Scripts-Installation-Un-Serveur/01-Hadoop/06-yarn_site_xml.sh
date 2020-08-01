#!/bin/bash

if [ $USER != "hdfs" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: hdfs"
        exit -1
fi

cat <<FIN_FICHIER > $HADOOP_HOME/etc/hadoop/yarn-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

  <property>
    <name>yarn.acl.enable</name>
    <value>true</value>
  </property>

  <property>
    <name>yarn.admin.acl</name>
    <value>yarn</value>
  </property>

    <property>
        <name>yarn.client.nodemanager-connect.max-wait-ms</name>
        <value>60000</value>
    </property>

    <property>
        <name>yarn.client.nodemanager-connect.retry-interval-ms</name>
        <value>10000</value>
    </property>

    <property>
        <name>yarn.http.policy</name>
        <value>HTTP_ONLY</value>
    </property>

    <property>
      <name>yarn.log-aggregation-enable</name>
      <value>true</value>
    </property>

    <property>
      <name>yarn.log-aggregation.retain-seconds</name>
      <value>2592000</value>
    </property>

    <property>
      <name>yarn.log.server.url</name>
      <value>http://`hostname -f`:19888/jobhistory/logs</value>
    </property>

    <property>
      <name>yarn.log.server.web-service.url</name>
      <value>http://`hostname -f`:8188/ws/v1/applicationhistory</value>
    </property>

    <property>
      <name>yarn.node-labels.enabled</name>
      <value>false</value>
    </property>

    <property>
      <name>yarn.nodemanager.address</name>
      <value>0.0.0.0:45454</value>
    </property>

    <property>
      <name>yarn.nodemanager.bind-host</name>
      <value>0.0.0.0</value>
    </property>

    <property>
      <name>yarn.nodemanager.delete.debug-delay-sec</name>
      <value>0</value>
    </property>

    <property>
      <name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name>
      <value>90</value>
    </property>

    <property>
      <name>yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb</name>
      <value>1000</value>
    </property>

    <property>
      <name>yarn.nodemanager.disk-health-checker.min-healthy-disks</name>
      <value>0.25</value>
    </property>

    <property>
      <name>yarn.nodemanager.health-checker.interval-ms</name>
      <value>135000</value>
    </property>

    <property>
      <name>yarn.nodemanager.health-checker.script.timeout-ms</name>
      <value>60000</value>
    </property>

    <property>
        <name>yarn.nodemanager.local-dirs</name>
        <value>/u01/hadoop/hdfs/nodemanager</value>
    </property>

    <property>
      <name>yarn.nodemanager.log-aggregation.compression-type</name>
      <value>gz</value>
    </property>

    <property>
      <name>yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds</name>
      <value>3600</value>
    </property>

    <property>
        <name>yarn.nodemanager.log-dirs</name>
        <value>/u01/hadoop/hdfs/journalnode</value>
    </property>

    <property>
        <name>yarn.nodemanager.log.retain-seconds</name>
        <value>604800</value>
    </property>

    <property>
        <name>yarn.nodemanager.recovery.dir</name>
        <value>/u01/hadoop/hdfs/recovery</value>
    </property>

    <property>
      <name>yarn.nodemanager.recovery.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>yarn.nodemanager.resource.cpu-vcores</name>
      <value>-1</value>
    </property>

    <property>
      <name>yarn.nodemanager.resource.memory-mb</name>
      <value>4096</value>
    </property>

    <property>
      <name>yarn.nodemanager.resource.percentage-physical-cpu-limit</name>
      <value>80</value>
    </property>

    <property>
       <name>yarn.nodemanager.runtime.linux.allowed-runtimes</name>
       <value>default</value>
     </property>

     <property>
       <name>yarn.nodemanager.vmem-check-enabled</name>
       <value>false</value>
     </property>

     <property>
       <name>yarn.nodemanager.vmem-pmem-ratio</name>
       <value>2.1</value>
     </property>

     <property>
       <name>yarn.nodemanager.webapp.cross-origin.enabled</name>
       <value>true</value>
     </property>


     <property>
       <name>yarn.resourcemanager.address</name>
       <value>`hostname -f`:8032</value>
     </property>

     <property>
       <name>yarn.resourcemanager.admin.address</name>
       <value>`hostname -f`:8033</value>
     </property>

     <property>
       <name>yarn.resourcemanager.am.max-attempts</name>
       <value>2</value>
     </property>

     <property>
       <name>yarn.resourcemanager.bind-host</name>
       <value>0.0.0.0</value>
     </property>

     <property>
       <name>yarn.resourcemanager.connect.max-wait.ms</name>
       <value>900000</value>
     </property>

     <property>
       <name>yarn.resourcemanager.connect.retry-interval.ms</name>
       <value>30000</value>
     </property>

     <property>
       <name>yarn.resourcemanager.display.per-user-apps</name>
       <value>true</value>
     </property>

     <property>
       <name>yarn.resourcemanager.ha.enabled</name>
       <value>false</value>
     </property>

    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>`hostname -f`</value>
    </property>

    <property>
      <name>yarn.resourcemanager.recovery.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>yarn.resourcemanager.scheduler.address</name>
      <value>`hostname -f`:8030</value>
    </property>

    <property>
      <name>yarn.resourcemanager.resource-tracker.address</name>
      <value>`hostname -f`:8031</value>
    </property>

    <property>
      <name>yarn.resourcemanager.scheduler.class</name>
      <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
    </property>

    <property>
      <name>yarn.resourcemanager.scheduler.monitor.enable</name>
      <value>true</value>
    </property>

    <property>
      <name>yarn.resourcemanager.state-store.max-completed-applications</name>
    </property>

    <property>
      <name>yarn.resourcemanager.store.class</name>
      <value>org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore</value>
    </property>

    <property>
      <name>yarn.resourcemanager.system-metrics-publisher.dispatcher.pool-size</name>
      <value>10</value>
    </property>

    <property>
      <name>yarn.resourcemanager.system-metrics-publisher.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>yarn.resourcemanager.webapp.address</name>
      <value>`hostname -f`:8088</value>
    </property>
    <property>
      <name>yarn.resourcemanager.webapp.cross-origin.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>yarn.resourcemanager.webapp.delegation-token-auth-filter.enabled</name>
      <value>false</value>
    </property>

    <property>
      <name>yarn.resourcemanager.webapp.https.address</name>
      <value>`hostname -f`:8090</value>
    </property>

    <property>
      <name>yarn.resourcemanager.work-preserving-recovery.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>yarn.resourcemanager.work-preserving-recovery.scheduling-wait-ms</name>
      <value>10000</value>
    </property>

    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
      <name>yarn.resourcemanager.zk-acl</name>
      <value>world:anyone:rwcda</value>
    </property>

    <property>
      <name>yarn.resourcemanager.zk-address</name>
      <value>`hostname -f`:2181</value>
    </property>

    <property>
      <name>yarn.resourcemanager.zk-num-retries</name>
      <value>1000</value>
    </property>

    <property>
      <name>yarn.resourcemanager.zk-retry-interval-ms</name>
      <value>1000</value>
    </property>

    <property>
      <name>yarn.resourcemanager.zk-state-store.parent-path</name>
      <value>/rmstore</value>
    </property>

    <property>
      <name>yarn.resourcemanager.zk-timeout-ms</name>
      <value>10000</value>
    </property>
    <property>
      <name>yarn.scheduler.maximum-allocation-mb</name>
      <value>8192</value>
    </property>

    <property>
      <name>yarn.scheduler.maximum-allocation-vcores</name>
      <value>4</value>
    </property>

    <property>
      <name>yarn.scheduler.minimum-allocation-mb</name>
      <value>1024</value>
    </property>

    <property>
      <name>yarn.scheduler.minimum-allocation-vcores</name>
      <value>1</value>
    </property>

    <property>
      <name>yarn.webapp.ui2.enable</name>
      <value>true</value>
    </property>

    <property>
      <name>yarn.timeline-service.http-cross-origin.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>yarn.resourcemanager.webapp.cross-origin.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>yarn.nodemanager.webapp.cross-origin.enabled</name>
      <value>true</value>
    </property>

  </configuration>
FIN_FICHIER
