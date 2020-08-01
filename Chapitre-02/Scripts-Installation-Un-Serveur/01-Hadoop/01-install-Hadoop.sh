#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

wget https://downloads.apache.org/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz
tar xzvf hadoop-3.3.0.tar.gz
rm hadoop-3.3.0.tar.gz
mv hadoop-3.3.0 hadoop
mv hadoop /usr/share
chown -R hdfs:hadoop /usr/share/hadoop
ls -al /usr/share/hadoop

rm -Rf /var/log/hadoop
mkdir /var/log/hadoop
chown -R hdfs:hadoop /var/log/hadoop
ls -al /var/log/hadoop

rm -Rf /var/log/yarn
mkdir /var/log/yarn
chown -R hdfs:hadoop /var/log/yarn
ls -al /var/log/yarn

cat << FIN_FICHIER > /etc/profile.d/hadoop.sh
#!/bin/bash
export HADOOP_INSTALL=/usr/share/hadoop
export HADOOP_HOME=\$HADOOP_INSTALL
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export HADOOP_YARN_HOME=\$HADOOP_HOME
export PATH=\$PATH:\$HADOOP_INSTALL/bin:\$HADOOP_INSTALL/sbin
FIN_FICHIER
