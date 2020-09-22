#!/bin/bash

cat <<FIN_FICHIER > $ZEPPELIN_HOME/conf/zeppelin-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

    <property>
      <name>zeppelin.cluster.addr</name>
      <value></value>
    </property>

    <property>
      <name>zeppelin.server.addr</name>
      <value>jupiter.olimp.fr</value>
    </property>

    <property>
      <name>zeppelin.server.port</name>
      <value>9999</value>
    </property>

    <property>
      <name>zeppelin.server.ssl.port</name>
      <value>9999</value>
    </property>

    <property>
      <name>zeppelin.server.context.path</name>
      <value>/</value>
    </property>

    <property>
      <name>zeppelin.war.tempdir</name>
      <value>webapps</value>
    </property>

    <property>
      <name>zeppelin.notebook.dir</name>
      <value>notebook</value>
    </property>

    <property>
      <name>zeppelin.interpreter.include</name>
      <value></value>
    </property>

    <property>
      <name>zeppelin.interpreter.exclude</name>
      <value></value>
    </property>

    <property>
      <name>zeppelin.notebook.homescreen</name>
      <value></value>
    </property>

    <property>
      <name>zeppelin.notebook.homescreen.hide</name>
      <value>false</value>
    </property>

    <property>
      <name>zeppelin.notebook.collaborative.mode.enable</name>
      <value>true</value>
    </property>

    <property>
      <name>zeppelin.notebook.storage</name>
      <value>org.apache.zeppelin.notebook.repo.GitNotebookRepo</value>
    </property>

    <property>
      <name>zeppelin.notebook.one.way.sync</name>
      <value>false</value>
    </property>

    <property>
      <name>zeppelin.interpreter.dir</name>
      <value>interpreter</value>
    </property>

    <property>
      <name>zeppelin.interpreter.localRepo</name>
      <value>local-repo</value>
    </property>

    <property>
      <name>zeppelin.interpreter.dep.mvnRepo</name>
      <value>https://repo1.maven.org/maven2/</value>
    </property>

    <property>
      <name>zeppelin.dep.localrepo</name>
      <value>local-repo</value>
    </property>

    <property>
      <name>zeppelin.helium.node.installer.url</name>
      <value>https://nodejs.org/dist/</value>
    </property>

    <property>
      <name>zeppelin.helium.npm.installer.url</name>
      <value>https://registry.npmjs.org/</value>
    </property>

    <property>
      <name>zeppelin.helium.yarnpkg.installer.url</name>
      <value>https://github.com/yarnpkg/yarn/releases/download/</value>
    </property>

    <property>
      <name>zeppelin.interpreter.group.default</name>
      <value>spark</value>
    </property>

    <property>
      <name>zeppelin.interpreter.connect.timeout</name>
      <value>60000</value>
    </property>

    <property>
      <name>zeppelin.interpreter.output.limit</name>
      <value>102400</value>
    </property>

    <property>
      <name>zeppelin.ssl</name>
      <value>false</value>
    </property>

    <property>
      <name>zeppelin.ssl.client.auth</name>
      <value>false</value>
    </property>

    <property>
      <name>zeppelin.ssl.keystore.path</name>
      <value>keystore</value>
    </property>

    <property>
      <name>zeppelin.ssl.keystore.type</name>
      <value>JKS</value>
    </property>

    <property>
      <name>zeppelin.ssl.keystore.password</name>
      <value>change me</value>
    </property>

    <property>
      <name>zeppelin.ssl.truststore.path</name>
      <value>truststore</value>
    </property>

    <property>
      <name>zeppelin.ssl.truststore.type</name>
      <value>JKS</value>
    </property>

    <property>
      <name>zeppelin.server.allowed.origins</name>
      <value>*</value>
    </property>

    <property>
      <name>zeppelin.username.force.lowercase</name>
      <value>false</value>
    </property>

    <property>
      <name>zeppelin.notebook.default.owner.username</name>
      <value>spark</value>
    </property>

    <property>
      <name>zeppelin.notebook.public</name>
      <value>true</value>
    </property>

    <property>
      <name>zeppelin.websocket.max.text.message.size</name>
      <value>10240000</value>
    </property>

    <property>
      <name>zeppelin.server.default.dir.allowed</name>
      <value>false</value>
    </property>

    <property>
      <name>zeppelin.interpreter.yarn.monitor.interval_secs</name>
      <value>10</value>
    </property>

    <property>
        <name>zeppelin.server.jetty.name</name>
        <value> </value>
    </property>

    <property>
      <name>zeppelin.server.xframe.options</name>
      <value>SAMEORIGIN</value>
    </property>

    <property>
      <name>zeppelin.server.xxss.protection</name>
      <value>1; mode=block</value>
    </property>

    <property>
      <name>zeppelin.server.xcontent.type.options</name>
      <value>nosniff</value>
    </property>

    <property>
      <name>zeppelin.run.mode</name>
      <value>local</value>
    </property>

    <property>
      <name>zeppelin.search.index.rebuild</name>
      <value>false</value>
    </property>

    <property>
      <name>zeppelin.search.use.disk</name>
      <value>true</value>
    </property>

    <property>
      <name>zeppelin.search.index.path</name>
      <value>/tmp/zeppelin-index</value>
    </property>

    <property>
      <name>zeppelin.jobmanager.enable</name>
      <value>false</value>
    </property>

    <property>
      <name>zeppelin.spark.only_yarn_cluster</name>
      <value>false</value>
    </property>

</configuration>
FIN_FICHIER
