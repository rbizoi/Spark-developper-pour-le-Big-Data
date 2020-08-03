#!/bin/bash

if [ $USER != "root" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: root"
        exit -1
fi

cd ~

wget https://downloads.apache.org/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz
tar xzvf spark-3.0.0-bin-hadoop3.2.tgz
rm -f spark-3.0.0-bin-hadoop3.2.tgz
mv spark-3.0.0-bin-hadoop3.2 spark
mv spark /usr/share

cat << FIN_FICHIER > /etc/profile.d/spark.sh
#!/bin/bash
export SPARK_HOME=/usr/share/spark
export PYSPARK_PYTHON=/bin/python3
#export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export PATH=\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$PATH
FIN_FICHIER

export SPARK_HOME=/usr/share/spark

cat <<FIN_FICHIER > $SPARK_HOME/conf/spark-env.sh
#!/usr/bin/env bash
SPARK_EXECUTOR_MEMORY="2G"
SPARK_DRIVER_MEMORY="1024M"
export SPARK_DAEMON_MEMORY=1024m
USER="\$(whoami)"
SPARK_IDENT_STRING=\$USER
SPARK_NICENESS=0
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8082
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_LOG_DIR=/var/log/spark/\$USER
export SPARK_PID_DIR=/var/run/spark/\$USER
FIN_FICHIER

cat <<FIN_FICHIER > $SPARK_HOME/conf/spark-defaults.conf
spark.serializer                     org.apache.spark.serializer.KryoSerializer
spark.io.compression.lz4.blockSize   128kb
#--------------------------------------------------------------------------------
spark.yarn.jars                      hdfs:///spark-jars
spark.yarn.am.cores                  1
#--------------------------------------------------------------------------------
spark.master yarn
spark.driver.memory                  1g
spark.executor.memory                2g
spark.executor.cores                 1
spark.executor.instances             2
spark.default.parallelism            2
#--------------------------------------------------------------------------------
spark.eventLog.dir hdfs:///spark-history/
spark.eventLog.enabled true
#--------------------------------------------------------------------------------
spark.history.fs.cleaner.enabled     true
spark.history.fs.cleaner.interval    7d
spark.history.fs.cleaner.maxAge      90d
spark.history.fs.logDirectory        hdfs:///spark-history/
FIN_FICHIER

cat <<FIN_FICHIER > $SPARK_HOME/conf/slaves
jupiter.olimp.fr
FIN_FICHIER

cp $SPARK_HOME/conf/log4j.properties $SPARK_HOME/conf/log4j.properties.sav
cat <<FIN_FICHIER > $SPARK_HOME/conf/log4j.properties
log4j.rootCategory=ERROR, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
log4j.logger.org.apache.spark.repl.Main=WARN
log4j.logger.org.sparkproject.jetty=WARN
log4j.logger.org.sparkproject.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain\$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop\$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
FIN_FICHIER



sudo chown -R spark:hadoop /usr/share/spark
ls -al /usr/share/spark

sudo rm -Rf /var/*/spark
sudo mkdir /var/log/spark
sudo mkdir /var/run/spark
sudo chown -R spark:hadoop /var/*/spark
ls -al /var/*/spark
