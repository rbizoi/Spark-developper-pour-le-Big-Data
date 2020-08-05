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
FIN_FICHIER

export HIVE_HOME=/usr/share/hive
export SPARK_HOME=/usr/share/spark

bash <(curl -s https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/03-Hive/03-hive_site_xml.sh)

wget https://cdn.mysql.com//Downloads/Connector-J/mysql-connector-java-8.0.20.tar.gz
tar -xvzf mysql-connector-java-8.0.20.tar.gz
cp mysql-connector-java-8.0.20/mysql-connector-java-8.0.20.jar $HIVE_HOME/lib
cp $HIVE_HOME/lib/mysql-connector-java-8.0.20.jar $SPARK_HOME/jars
cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml
cp $HIVE_HOME/conf/hive-env.sh $SPARK_HOME/conf/hive-env.sh
chown -R spark:hadoop $SPARK_HOME
chown -R hive:hadoop $HIVE_HOME
rm mysql-connector-java-8.0.20.tar.gz
rm -Rf mysql-connector-java-8.0.20

ls -al /usr/share/hive/
