from pyspark.sql import SparkSession

spark = SparkSession.builder.\
        appName("Les RDD").getOrCreate()

spark.sparkContext

data = [1, 'Razvan', True, ['44, rue Paul Claudel','Strasbourg']]
distData = spark.sparkContext.parallelize(data)

(distData , distData.collect())
