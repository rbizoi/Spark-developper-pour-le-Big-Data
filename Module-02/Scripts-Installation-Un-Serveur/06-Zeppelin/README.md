repGitHub=https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/Chapitre-01/Scripts-Installation-Un-Serveur/06-Zeppelin

bash <(curl -s $repGitHub/01-installation-zookeeper.sh)


wget https://mega.nz/file/OOhBjZBJ#8Ocuwx7u67I_ci74pnKzaHwaBUtUeispYxT4jzpDGec



zeppelin-daemon.sh start
zeppelin-daemon.sh start
zeppelin-daemon.sh status
zeppelin-daemon.sh restart
zeppelin-daemon.sh stop

sudo netstat -lnp | grep 9999
