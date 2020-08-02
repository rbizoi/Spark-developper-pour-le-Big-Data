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

cat <<FIN_FICHIER > /usr/share/zookeeper/conf/zoo.cfg
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

cat << FIN_FICHIER > /etc/profile.d/zookeeper.sh
#!/bin/bash
# Configuration Zookeeper
export ZK_HOME=/usr/share/zookeeper
export ZK_HOSTS=`hostname -f`:2181
export PATH=\$ZK_HOME/bin:\$PATH
FIN_FICHIER

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
ExecStart=/usr/share/zookeeper/bin/zkServer.sh start /usr/share/zookeeper/conf/zoo.cfg
ExecStop=/usr/share/zookeeper/bin/zkServer.sh stop /usr/share/zookeeper/conf/zoo.cfg
ExecReload=/usr/share/zookeeper/bin/zkServer.sh restart /usr/share/zookeeper/conf/zoo.cfg
TimeoutSec=30
Restart=on-failure

[Install]
WantedBy=default.target
FIN_FICHIER

systemctl start zookeeper
systemctl enable zookeeper

netstat -plnt | grep 2181
