spark.sql("show databases").show()
spark.sql("use coursspark3")
spark.sql("show tables").show()
spark.sql("select * from coursspark3.employes where ID < 8000 limit 3").show()
