repGitHub=https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/00-Prerequis

# 8.1.	Installer les prérequis
## 8.1.1.	La configuration du système d'exploitation
## 8.1.2.	L'installation de l'environnement Java
## 8.1.3.	L'installation du langage Python
## 8.1.4.	L'installation du langage R


bash <(curl -s $repGitHub/01-systme-exploitation.sh)

## 8.1.5.	La création des utilisateurs

bash <(curl -s $repGitHub/02-creation-utilisateurs.sh)



for nom in "hdfs" "hive" "zookeeper" "spark" "kafka" "zeppelin"
do
   `printf "userdel %s -r \n" $nom`
done


for nom in "hadoop" "hdfs" "hive" "zookeeper" "spark" "kafka" "zeppelin"
do
   `printf "groupdel %s\n" $nom`
done





CoursSPARK#
su - hdfs
rm -Rf .ssh
/usr/bin/ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' -P ''
/usr/bin/ssh-copy-id -i ~/.ssh/id_rsa.pub jupiter.olimp.fr
ssh jupiter date
exit
su - hive
rm -Rf .ssh
/usr/bin/ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' -P ''
/usr/bin/ssh-copy-id -i ~/.ssh/id_rsa.pub jupiter.olimp.fr
ssh jupiter date
exit
su - spark
rm -Rf .ssh
/usr/bin/ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' -P ''
/usr/bin/ssh-copy-id -i ~/.ssh/id_rsa.pub jupiter.olimp.fr
ssh jupiter date
exit
su - kafka
rm -Rf .ssh
/usr/bin/ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' -P ''
/usr/bin/ssh-copy-id -i ~/.ssh/id_rsa.pub jupiter.olimp.fr
ssh jupiter date
exit
su - zookeeper
rm -Rf .ssh
/usr/bin/ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' -P ''
/usr/bin/ssh-copy-id -i ~/.ssh/id_rsa.pub jupiter.olimp.fr
ssh jupiter date
exit
su - zeppelin
rm -Rf .ssh
/usr/bin/ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' -P ''
/usr/bin/ssh-copy-id -i ~/.ssh/id_rsa.pub jupiter.olimp.fr
ssh jupiter date
exit
