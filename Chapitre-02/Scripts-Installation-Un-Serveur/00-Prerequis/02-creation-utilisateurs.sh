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
chmod 777 /tmp/init_cle.sh

pass=CoursSPARK#
for nom in "hdfs" "hive" "zookeeper" "spark" "kafka" "zeppelin"
do
    `echo -e "$pass\n$pass"|passwd $nom`
    `su -c /tmp/init_cle.sh - $nom`
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

cat <<FIN_FICHIER > /tmp/copy_cle.sh
cat /dev/zero | /usr/bin/sshpass -p "CoursSPARK#" /usr/bin/ssh-copy-id `hostname -f`
cat /dev/zero | ssh `hostname -f` date
cat /dev/zero | ssh jupiter date
FIN_FICHIER
chmod 777 /tmp/copy_cle.sh

for nom in "hdfs" "hive" "zookeeper" "spark" "kafka" "zeppelin"
do
cat <<FIN_FICHIER > /home/$nom/.ssh/config
StrictHostKeyChecking no
FIN_FICHIER
`chown $nom:hadoop /home/$nom/.ssh/config`
`su -c /tmp/copy_cle.sh - $nom`
`rm /home/$nom/.ssh/config`
done

rm /tmp/copy_cle.sh
