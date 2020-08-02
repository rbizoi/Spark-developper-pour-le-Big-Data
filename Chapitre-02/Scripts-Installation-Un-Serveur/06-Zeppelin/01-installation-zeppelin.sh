#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~


#wget ...
tar xzvf zeppelin-0.9.0-SNAPSHOT.tar.gz
mv zeppelin-0.9.0-SNAPSHOT zeppelin
rm zeppelin-0.9.0-SNAPSHOT.tar.gz
sudo mv zeppelin /usr/share

cp $ZEPPELIN_HOME/conf/zeppelin-site.xml.template $ZEPPELIN_HOME/conf/zeppelin-site.xml




sudo chown -R zeppelin:hadoop /usr/share/zeppelin
ll /usr/share/zeppelin

sudo rm -Rf /var/log/zeppelin
sudo mkdir /var/log/zeppelin
sudo mkdir -p /var/run/zeppelin/zeppelin
sudo chown -R zeppelin:hadoop /var/*/zeppelin

ll /var/*/zeppelin


cat << FIN_FICHIER > /etc/profile.d/zeppelin.sh
#!/bin/bash
# Configuration Zeppelin
export ZEPPELIN_HOME=/usr/share/zeppelin
export PATH=\$ZEPPELIN_HOME/bin:\$PATH
FIN_FICHIER


zeppelin-daemon.sh start
zeppelin-daemon.sh start
zeppelin-daemon.sh status
zeppelin-daemon.sh restart
zeppelin-daemon.sh stop

sudo netstat -lnp | grep 9995
