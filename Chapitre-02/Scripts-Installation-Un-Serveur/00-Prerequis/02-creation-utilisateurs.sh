#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

#-----------------------------------------------------------------------------------------------
# 8.1.5.	La création des utilisateurs
#-----------------------------------------------------------------------------------------------

nrid=54000

for nom in "hadoop" "hdfs" "hive" "zookeeper" "spark" "kafka" "zeppelin"
do
   `printf "groupadd -g %5d %s\n" $nrid $nom`
   ((nrid++))
done

nrid=54001
for nom in "hdfs" "hive" "zookeeper" "spark" "kafka" "zeppelin"
do
   `printf "useradd %s --uid %5d --home /home/%s/ --create-home --gid hadoop --groups hadoop,hdfs,hive,zookeeper,spark,kafka,zeppelin,sudo --shell /bin/bash \n" $nom $nrid $nom`
   ((nrid++))
done


cat <<FIN_FICHIER > /tmp/init_cle.sh
cat /dev/zero | /usr/bin/ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' -P ''
FIN_FICHIER

pass=CoursSPARK#
for nom in "hdfs" "hive" "zookeeper" "spark" "kafka" "zeppelin"
do
    `echo -e "$pass\n$pass"|passwd $nom`
    `cp /tmp/init_cle.sh /home/$nom/init_cle.sh`
    `chmod 777 /home/$nom/init_cle.sh`
    `su -c /home/$nom/init_cle.sh - $nom`
     rm /home/$nom/init_cle.sh
done

rm /tmp/init_cle.sh

for nom in "hive" "zookeeper" "spark" "kafka" "zeppelin"
do
    `mkdir -p /usr/share/$nom`
     chown -R $nom:hdfs /usr/share/$nom
done

mkdir -p /usr/share/hadoop
chown -R hdfs:hadoop /usr/share/hadoop

mkdir -p /u01/hadoop/hdfs/namenode
mkdir -p /u01/hadoop/hdfs/checkpoint
mkdir -p /u01/hadoop/hdfs/journalnode
mkdir -p /u01/hadoop/hdfs/datanode
mkdir -p /u01/hadoop/hdfs/nodemanager
mkdir -p /u01/hadoop/hdfs/recovery
chown -R hdfs:hadoop /u01/hadoop
