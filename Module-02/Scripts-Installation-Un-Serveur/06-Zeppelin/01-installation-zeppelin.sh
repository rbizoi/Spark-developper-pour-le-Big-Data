#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

wget https://edef4.pcloud.com/cfZbVwv0ZIqIWJZvQYZZ76Qm37Z2ZZRR5ZZydJMU7Zp7ZJ7Z2ZuZqZ47ZTZtZaZQ7ZrZhZDZNZuFKRQgDk125Ch3HX85t2opvYmm5k/zeppelin-0.9.0-SNAPSHOT.tar.gz
tar xzvf zeppelin-0.9.0-SNAPSHOT.tar.gz
mv zeppelin-0.9.0-SNAPSHOT zeppelin
rm zeppelin-0.9.0-SNAPSHOT.tar.gz
sudo mv zeppelin /usr/share

bash <(curl -s https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-01/Scripts-Installation-Un-Serveur/04-Zookeeper/02-zeppelin_site_xml.sh)
bash <(curl -s https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-01/Scripts-Installation-Un-Serveur/04-Zookeeper/03-zeppelin_env_sh.sh)

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

cat <<FIN_FICHIER > /etc/systemd/system/zeppelin.service
[Unit]
Description=Apache Zeppelin daemon
After=syslog.target network.target

[Service]
Type=oneshot
User=zeppelin
Group=hadoop
ExecStart=$ZEPPELIN_HOME/bin/zeppelin-daemon.sh start
ExecStop=$ZEPPELIN_HOME/bin/zeppelin-daemon.sh stop
ExecReload=$ZEPPELIN_HOME/bin/zeppelin-daemon.sh restart
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
FIN_FICHIER

# uniquement si le clauster est demmaré autrement il faut attendre
# il necessite la création des répértoires
systemctl start zeppelin
systemctl disable zeppelin
