#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

wget https://downloads.apache.org/zookeeper/zookeeper-3.6.1/apache-zookeeper-3.6.1-bin.tar.gz
tar xzvf apache-zookeeper-3.6.1-bin.tar.gz
rm -f apache-zookeeper-3.6.1-bin.tar.gz
mv apache-zookeeper-3.6.1-bin zookeeper
mv zookeeper /usr/share


cat << FIN_FICHIER > /etc/profile.d/zookeeper.sh
#!/bin/bash
# Configuration Zookeeper
export ZOOKEEPER_HOME=/usr/share/zookeeper
export ZOOKEEPER_HOSTS=`hostname -f`:2181
export PATH=\$ZOOKEEPER_HOME/bin:\$PATH
FIN_FICHIER

export ZOOKEEPER_HOME=/usr/share/zookeeper

cat <<FIN_FICHIER > $ZOOKEEPER_HOME/conf/zoo.cfg
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

chown -R zookeeper:hadoop /usr/share/zookeeper
ls -al /usr/share/zookeeper/

rm -Rf /var/log/zookeeper
rm -Rf /var/zookeeper
sudo mkdir /var/log/zookeeper
sudo mkdir /var/zookeeper

cat <<FIN_FICHIER > /var/zookeeper/myid
1
FIN_FICHIER

sudo chown -R zookeeper:hadoop /var/*/zookeeper
sudo chown -R zookeeper:hadoop /var/zookeeper
ll /var/zookeeper
ll /var/log/zookeeper

cat <<FIN_FICHIER > /etc/systemd/system/zookeeper.service
[Unit]
Description=Zookeeper Daemon
Documentation=http://zookeeper.apache.org
Requires=network.target
After=network.target

[Service]
Type=forking
WorkingDirectory=/usr/share/zookeeper
User=zookeeper
Group=hadoop
ExecStart=$ZOOKEEPER_HOME/bin/zkServer.sh start $ZOOKEEPER_HOME/conf/zoo.cfg
ExecStop=$ZOOKEEPER_HOME/bin/zkServer.sh stop $ZOOKEEPER_HOME/conf/zoo.cfg
ExecReload=$ZOOKEEPER_HOME/bin/zkServer.sh restart $ZOOKEEPER_HOME/conf/zoo.cfg
TimeoutSec=30
Restart=on-failure

[Install]
WantedBy=default.target
FIN_FICHIER

systemctl start zookeeper
systemctl disable zookeeper

netstat -plnt | grep 2181
