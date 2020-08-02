#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

wget https://downloads.apache.org/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz
tar xzvf hadoop-3.3.0.tar.gz
rm hadoop-3.3.0.tar.gz
mv hadoop-3.3.0 hadoop
mv hadoop /usr/share
chown -R hdfs:hadoop /usr/share/hadoop
ls -al /usr/share/hadoop

rm -Rf /var/log/hadoop
rm -Rf /var/run/hadoop
mkdir /var/log/hadoop
mkdir /var/run/hadoop
chown -R hdfs:hadoop /var/*/hadoop
ls -al /var/*/hadoop

rm -Rf /var/log/yarn
rm -Rf /var/run/yarn
mkdir /var/log/yarn
mkdir /var/run/yarn
chown -R hdfs:hadoop /var/*/yarn
ls -al /var/*/yarn

cat << FIN_FICHIER > /etc/profile.d/hadoop.sh
#!/bin/bash
export PDSH_RCMD_TYPE=ssh
export HADOOP_INSTALL=/usr/share/hadoop
export HADOOP_HOME=\$HADOOP_INSTALL
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export HADOOP_YARN_HOME=\$HADOOP_HOME
export PATH=\$PATH:\$HADOOP_INSTALL/bin:\$HADOOP_INSTALL/sbin
FIN_FICHIER
