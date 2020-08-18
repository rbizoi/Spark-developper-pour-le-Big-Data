
export PYSPARK_DRIVER_PYTHON=python3

python scripts/01-charge_tweets.py &

spark-submit \
  --master spark://jupiter.olimp.fr:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
scripts/02-lire-kafka-tweets.py
