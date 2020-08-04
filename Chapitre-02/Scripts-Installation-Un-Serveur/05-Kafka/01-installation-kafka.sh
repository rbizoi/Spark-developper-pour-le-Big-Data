#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

wget https://downloads.apache.org/kafka/2.5.0/kafka_2.12-2.5.0.tgz
tar xzvf kafka_2.12-2.5.0.tgz
rm -f kafka_2.12-2.5.0.tgz
mv kafka_2.12-2.5.0 kafka
mv kafka /usr/share

cat << FIN_FICHIER > /etc/profile.d/kafka.sh
#!/bin/bash
# Configuration Zookeeper
export KAFKA_HOME=/usr/share/kafka
#export KAFKA_MANAGER_HOME=/usr/share/kafka-manager
export PATH=\$KAFKA_HOME/bin:\$PATH
FIN_FICHIER

export KAFKA_HOME=/usr/share/kafka

cat <<FIN_FICHIER > $KAFKA_HOME/config/zookeeper.properties
tickTime=2000
dataDir=/var/zookeeper
dataLogDir=/var/log/zookeeper
clientPort=2181
maxClientCnxns=60
initLimit=10
syncLimit=5
server.1=`hostname -f`:2888:3888
autopurge.purgeInterval=1
metricsProvider.className=org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider
metricsProvider.httpPort=7000
metricsProvider.exportJvmInfo=true
FIN_FICHIER

cat <<FIN_FICHIER > $KAFKA_HOME/config/server.properties
broker.id=1
listeners=PLAINTEXT://`hostname -f`:9092
log.cleanup.interval.mins=10
log.retention.bytes=-1
log.retention.check.interval.ms=600000
log.retention.hours=168
log.roll.hours=168
log.segment.bytes=1073741824
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/var/log/kafka
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
zookeeper.connect=`hostname -f`:2181
zookeeper.connection.timeout.ms=25000
offset.metadata.max.bytes=4096
offsets.commit.required.acks=-1
offsets.commit.timeout.ms=5000
offsets.load.buffer.size=5242880
offsets.retention.check.interval.ms=600000
offsets.retention.minutes=1440
offsets.topic.compression.codec=0
offsets.topic.num.partitions=50
offsets.topic.replication.factor=1
offsets.topic.segment.bytes=104857600
port=19092
delete.topic.enable=true
FIN_FICHIER

chown -R kafka:hadoop /usr/share/kafka
ls -al /usr/share/kafka/

rm -Rf /var/log/kafka
mkdir /var/log/kafka
chown -R kafka:hadoop /var/log/kafka
ll /var/log/kafka

cat <<FIN_FICHIER > /etc/systemd/system/kafka.service
[Unit]
Description=Kafka Daemon
Documentation=http://kafka.apache.org
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=forking
WorkingDirectory=$KAFKA_HOME
User=kafka
Group=hadoop
ExecStart=$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
ExecStop=$KAFKA_HOME/bin/kafka-server-stop.sh
TimeoutSec=30
Restart=on-failure

[Install]
WantedBy=default.target
FIN_FICHIER

systemctl start kafka
systemctl enable kafka

netstat -plnt | grep 9092
