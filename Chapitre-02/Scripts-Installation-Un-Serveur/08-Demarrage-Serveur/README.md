

sudo su -
mkdir start-stop-cluster
cd start-stop-cluster

wget https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/08-Demarrage-Serveur/01-start-cluster.sh

wget https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-02/Scripts-Installation-Un-Serveur/08-Demarrage-Serveur/01-stop-cluster.sh

. 01-start-cluster.sh
