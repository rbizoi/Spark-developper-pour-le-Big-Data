repGitHub=https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-03/jars

cd $SPARK_HOME/jars
wget $repGitHub/delta-core_2.12-0.8.0-SNAPSHOT.jar
wget $repGitHub/ojdbc8.jar
wget $repGitHub/mssql-jdbc-8.4.0.jre8.jar
wget $repGitHub/spark-mssql-connector_2.12-1.0.0.jar

wget $repGitHub/postgresql-42.2.10.jar

hdfs dfs -put delta-core_2.12-0.8.0-SNAPSHOT.jar /spark-jars
hdfs dfs -put ojdbc8.jar /spark-jars
hdfs dfs -put mssql-jdbc-8.4.0.jre8.jar /spark-jars
hdfs dfs -put spark-mssql-connector_2.12-1.0.0.jar /spark-jars

hdfs dfs -put postgresql-42.2.10.jar /spark-jars

hdfs dfs -ls /spark-jars/delta-core_2.12-0.8.0-SNAPSHOT.jar
hdfs dfs -ls ojdbc8.jar /spark-jars/ojdbc8.jar
hdfs dfs -ls /spark-jars/postgresql-42.2.10.jar
hdfs dfs -ls /spark-jars/mssql-jdbc-8.4.0.jre8.jar
hdfs dfs -ls /spark-jars/spark-mssql-connector_2.12-1.0.0.jar

besoin imperativement du driver mssql-jdbc-8.4.0.jre8.jar
git clone https://github.com/microsoft/sql-spark-connector.git
intelij pour la première fois a besoin d'initialiser JDK
configurer build.sbt et pom.xml pour scala 1.12.11 et spark 3.0.0
sbt> package
ls -al target/scala-1.12
