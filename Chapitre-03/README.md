
export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_DRIVER_PYTHON_OPTS=''


spark-shell \
    --master spark://jupiter.olimp.fr:7077 \
    --executor-cores 1 \
    --executor-memory 1g

    
