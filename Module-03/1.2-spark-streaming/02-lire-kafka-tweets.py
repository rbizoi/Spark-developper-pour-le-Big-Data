#groupId = org.apache.spark
#artifactId = spark-sql-kafka-0-10_2.12
#version = 3.0.0
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,length,lower,regexp_replace

spark = SparkSession.builder.\
        appName("Lecture  du flux des tweets").getOrCreate()

tweets = spark\
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.9.112:9092") \
    .option("subscribe", "tweets-flux") \
    .load()

words = tweets.select(explode(split(
                               regexp_replace(lower(tweets.value),
                                             '[^a-z]',' '), ' ')).\
                               alias('word'))
wordsF = words.filter(length(words.word)>3)
wordCounts = wordsF.groupBy('word').count()

query = wordCounts.orderBy('count', ascending=False)\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()

query.awaitTermination()
