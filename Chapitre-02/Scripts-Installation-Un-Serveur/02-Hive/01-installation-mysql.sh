#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

systemctl status mysql

mot_de_passe=CoursSPARK#
mysql_secure_installation <<FIN_FICHIER
y
2
${mot_de_passe}
${mot_de_passe}
y
y
y
y
y
FIN_FICHIER

sed -i -e "s/127.0.0.1/0.0.0.0/g" /etc/mysql/mysql.conf.d/mysqld.cnf
cat /etc/mysql/mysql.conf.d/mysqld.cnf  | grep bind-address

systemctl restart mysql


mysql -e "SET PASSWORD FOR root@localhost = PASSWORD('CoursSPARK#');FLUSH PRIVILEGES;"


#!/bin/bash
dpkg -s expect 2> /dev/null
if [ $? == 0 ] ; then :
else apt-get install -y expect
fi
MYSQL_ROOT_PASSWORD=""
SECURE_MYSQL=$(expect -c "
set timeout 10
spawn mysql_secure_installation
expect \"Enter current password for root (enter for none):\"
send \"${MYSQL_ROOT_PASSWORD}\r\"
expect \"Change the root password?\"
send \"n\r\"
expect \"Remove anonymous users?\"
send \"y\r\"
expect \"Disallow root login remotely?\"
send \"y\r\"
expect \"Remove test database and access to it?\"
send \"y\r\"
expect \"Reload privilege tables now?\"
send \"y\r\"
expect EOF
")
echo ${SECURE_MYSQL}
New password:
Re-enter new password:
Do you wish to continue with the password provided?(Press y|Y for Yes, any other key for No) :
