spark.sql("show databases").show()
spark.sql("use cours_spark")
spark.sql("show tables").show()
spark.sql("select * from cours_spark.postesmeteo where ID < 8000 limit 3").show()
