repGitHub=https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/02-Spark

bash <(curl -s $repGitHub/01-installation-spark.sh)

su - Spark

repGitHub=https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/02-Spark

bash <(curl -s $repGitHub/02-initialise-spark.sh)
bash <(curl -s $repGitHub/03-initialise-jupyter-notebook.sh)

spark-shell   --master spark://jupiter.olimp.fr:7077
# jupyter notebook
pyspark   --master spark://jupiter.olimp.fr:7077
spark-sql   --master spark://jupiter.olimp.fr:7077


# Controle installation Apache Spark

bash <(curl -s $repGitHub/import-meteo.sh)
