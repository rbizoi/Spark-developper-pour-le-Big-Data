repGitHub=https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-03/jars

cd $SPARK_HOME/jars
wget $repGitHub/delta-core_2.12-0.8.0-SNAPSHOT.jar
wget $repGitHub/spark-mssql-connector_2.12-1.0.0.jar
wget $repGitHub/postgresql-42.2.10.jar


hdfs dfs -put postgresql-42.2.10.jar /spark-jars
hdfs dfs -put delta-core_2.12-0.8.0-SNAPSHOT.jar /spark-jars
hdfs dfs -put spark-mssql-connector_2.12-1.0.0.jar /spark-jars

hdfs dfs -ls /spark-jars/postgresql-42.2.10.jar
hdfs dfs -ls /spark-jars/delta-core_2.12-0.8.0-SNAPSHOT.jar
hdfs dfs -ls /spark-jars/spark-mssql-connector_2.12-1.0.0.jar
