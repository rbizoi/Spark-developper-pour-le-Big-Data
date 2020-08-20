from pyspark.sql.functions import *

villes.select('ville',
               lpad('ville', 12, "-").alias('lpad'),
               rpad('ville', 12, "-").alias('rpad'),
               lpad(rpad('ville', 18, "-"), 20, "-").alias('pad'),
               trim(lpad(rpad('ville', 18, ' '), 20, ' ')).alias('trim'),
               )\
      .show(truncate=False)


trim('name).as("trim"),
ltrim('name).as("ltrim"),
lpad('trim, 8, "-").as("lpad"),
rpad('trim, 8, "=").as("rpad"))
