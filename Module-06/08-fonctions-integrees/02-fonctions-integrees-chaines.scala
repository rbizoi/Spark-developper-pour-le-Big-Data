import org.apache.spark.sql.functions._

villes.select('ville,round('altitude,-2).alias("altitude")).
      groupBy("altitude").
      agg(collect_list('ville).alias("ville par altitude")).
      show(truncate=false)
