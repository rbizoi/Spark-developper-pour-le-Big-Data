#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

wget https://downloads.apache.org/hive/hive-2.3.7/apache-hive-2.3.7-bin.tar.gz
tar xzvf apache-hive-2.3.7-bin.tar.gz
rm -f apache-hive-2.3.7-bin.tar.gz
mv apache-hive-2.3.7-bin      hive
sudo mv hive                  /usr/share

cat << FIN_FICHIER > /etc/profile.d/hive.sh
#!/bin/bash
export HIVE_HOME=/usr/share/hive
export HIVE_CONF_DIR=\$HIVE_HOME/conf
export PATH=\$PATH:\$HIVE_HOME/bin
FIN_FICHIER

cat <<FIN_FICHIER > $HIVE_HOME/conf/hive-env.sh
export HADOOP_HOME=/usr/share/hadoop
export HIVE_CONF_DIR=/usr/share/hive/conf
export CLASSPATH=\$CLASSPATH:\${HIVE_HOME}/lib/*:.
FIN_FICHIER

export HIVE_HOME=/usr/share/hive
wget https://cdn.mysql.com//Downloads/Connector-J/mysql-connector-java-8.0.20.tar.gz
tar -xvzf mysql-connector-java-8.0.20.tar.gz
cp mysql-connector-java-8.0.20/mysql-connector-java-8.0.20.jar $HIVE_HOME/lib
rm mysql-connector-java-8.0.20.tar.gz
rm -Rf mysql-connector-java-8.0.20



ls -al /usr/share/hive/
