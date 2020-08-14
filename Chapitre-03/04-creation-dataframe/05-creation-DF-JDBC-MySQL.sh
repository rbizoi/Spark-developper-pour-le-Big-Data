cat << FIN_FICHIER > create-database.mysql.sql
DROP DATABASE IF EXISTS coursSPARK;
CREATE DATABASE coursSPARK;
GRANT ALL PRIVILEGES ON coursSPARK.* TO 'spark'@'localhost';
GRANT ALL PRIVILEGES ON coursSPARK.* TO 'spark'@'%';
FLUSH PRIVILEGES;
USE coursSPARK;
FIN_FICHIER

mysql --user=root < create-database.mysql.sql > create-database.mysql.txt
rm create-database.mysql.sql create-database.mysql.txt

cat << FIN_FICHIER > test-database.mysql.sql
SHOW CHARACTER SET;
SHOW DATABASES;
SHOW GRANTS FOR spark;
SHOW PRIVILEGES;
FIN_FICHIER

mysql --user=spark --password='CoursSPARK3#' --database=coursSPARK < test-database.mysql.sql > test-database.mysql.txt
rm test-database.mysql.sql test-database.mysql.txt
