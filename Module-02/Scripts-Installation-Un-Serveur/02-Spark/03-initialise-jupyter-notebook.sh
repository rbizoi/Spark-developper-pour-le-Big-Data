#!/bin/bash

if [ $USER != "spark" ]; then
        echo "Le script doit être exécuté en tant qu'utilisateur: spark"
        exit -1
fi

cd ~

jupyter notebook --generate-config

sed -i -e "s/#c.NotebookApp.ip = 'localhost'/c.NotebookApp.ip = '`hostname -f`'/g" /home/spark/.jupyter/jupyter_notebook_config.py
sed -i -e "s/#c.NotebookApp.port = 8888/c.NotebookApp.port = 8888/g" /home/spark/.jupyter/jupyter_notebook_config.py
