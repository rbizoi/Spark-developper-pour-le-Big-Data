#!/bin/bash

if [ $USER != "hadoop" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: hadoop"
        exit -1
fi


cp $HADOOP_CONF_DIR/yarn-env.sh $HADOOP_CONF_DIR/yarn-env.sh.sav
cp $HADOOP_CONF_DIR/hadoop-env.sh $HADOOP_CONF_DIR/hadoop-env.sh.sav

cat <<FIN_FICHIER > $HADOOP_CONF_DIR/hadoop-env.sh
export JAVA_HOME=$JAVA_HOME/jre
export HADOOP_INSTALL=/usr/share/hadoop
export HADOOP_HOME=\${HADOOP_INSTALL}
export HADOOP_CONF_DIR=\${HADOOP_HOME}/etc/hadoop
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true \${HADOOP_OPTS}"
export HADOOP_OS_TYPE=\${HADOOP_OS_TYPE:-\$(uname -s)}
export CLASSPATH=\$CLASSPATH:\${HADOOP_HOME}/lib/*:.
export HADOOP_HEAPSIZE="1024"
export HADOOP_NAMENODE_INIT_HEAPSIZE="-Xms1024m"
USER="\$(whoami)"
export HADOOP_CLIENT_OPTS="-Xmx\${HADOOP_HEAPSIZE}m \$HADOOP_CLIENT_OPTS"
export HADOOP_LOG_DIR=/var/log/hadoop/\$USER
FIN_FICHIER

cat <<FIN_FICHIER > $HADOOP_CONF_DIR/yarn-env.sh
export JAVA_HOME=$JAVA_HOME/jre
export HADOOP_LOG_DIR=/var/log/yarn/yarn
export HADOOP_LIBEXEC_DIR=\${HADOOP_INSTALL}/libexec
YARN_HEAPSIZE=250
export YARN_RESOURCEMANAGER_HEAPSIZE=250
export YARN_NODEMANAGER_HEAPSIZE=512
export YARN_NODEMANAGER_OPTS="\$YARN_NODEMANAGER_OPTS -Dnm.audit.logger=INFO,NMAUDIT"
export YARN_RESOURCEMANAGER_OPTS="\$YARN_RESOURCEMANAGER_OPTS -Dyarn.server.resourcemanager.appsummary.logger=INFO,RMSUMMARY -Drm.audit.logger=INFO,RMAUDIT"
FIN_FICHIER
