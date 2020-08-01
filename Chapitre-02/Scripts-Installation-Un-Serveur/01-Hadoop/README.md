# 8.2.	Installer Apache Hadoop

repGitHub=https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/01-Hadoop

bash <(curl -s $repGitHub/01-install-Hadoop.sh)

# 8.2.1	Configurer Apache Hadoop

su - hdfs

Les fichiers de configuration que on va modifier sont :
        •	hadoop-env.sh
        •	yarn-env.sh
        •	core-site.xml
        •	hdfs-site.xml
        •	mapred-site.xml
        •	yarn-site.xml


repGitHub=https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/01-Hadoop

bash <(curl -s $repGitHub/02-fichiers-environnement.sh)
bash <(curl -s $repGitHub/03-core_site_xml.sh)
bash <(curl -s $repGitHub/04-hdfs_site_xml.sh)
bash <(curl -s $repGitHub/05-mapred_site_xml.sh)
bash <(curl -s $repGitHub/06-yarn_site_xml.sh)

# 8.2.2	Démarer Apache Hadoop

bash <(curl -s $repGitHub/06-yarn_site_xml.sh)

bash <(curl -s $repGitHub/07-initialise-hadoop.sh)
