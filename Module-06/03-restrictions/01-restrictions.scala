import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

meteoDataFrame.count()
meteoDataFrame.selectExpr(
       "numer_sta","t  - 273.15 as temperature"
       ).limit(5).show()

meteoDataFrame.select("numer_sta").count()
meteoDataFrame.select("numer_sta").distinct.count()
meteoDataFrame.select("numer_sta").distinct.show(3)

villes.where('latitude > 49).show()

villes.where("latitude > 49" and "altitude > 90").show()
villes.where("latitude > 49" or "altitude > 90").show()

villes.sample(0.02,0).show()

villes.filter("Id == '07168' or Id == '07280'").show()
villes.filter("Id = '07168' or Id = '07280'").show()
