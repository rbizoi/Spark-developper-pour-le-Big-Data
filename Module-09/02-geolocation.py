from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import unicodedata

@udf("string")
def nettoyer(colonne):
    nk = unicodedata.normalize('NFKD', colonne)
    return str(nk.encode('ASCII', 'ignore').decode('ASCII'))


#clients     enregistrements = 14994     15034   14994   15034
clients     = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/clients`")\
                    .select(col('customer_id').alias('id'),
                            col('customer_unique_id').alias('unique_id'),
                            col('customer_zip_code_prefix').alias('code_postal'),
                            col('customer_city').alias('ville'),
                            col('customer_state').alias('etat'))\
                    .cache()
#vendeurs     enregistrements = 2246     2280    2264    2296
vendeurs    = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/vendeurs`")\
                    .select(col('seller_id').alias('id'),
                            col('seller_zip_code_prefix').alias('code_postal'),
                            col('seller_city').alias('ville'),
                            col('seller_state').alias('etat'))\
                    .cache()

geolocation = spark.sql("select * from \
                         parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/geolocation`")\
                    .select(col('geolocation_zip_code_prefix').alias('code_postal'),
                                nettoyer('geolocation_city').alias('ville'),
                                nettoyer('geolocation_state').alias('etat'),
                                col('geolocation_lat').alias('latitude'),
                                col('geolocation_lng').alias('longitude'))\
                    .cache()
geolocation.show()

print('enregistrements -- clients              = %d\n\
id-code_postal,id                       = %d\t%d\n\
unique_id-code_postal,unique_id         = %d\t%d\n\
code_postal-code_postal,ville           = %d\t%d\n\
code_postal,etat-code_postal,ville,etat = %d\t%d'%(
         clients.count(),
         clients.select('id').distinct().count(),
         clients.groupBy('code_postal','id').count().count(),
         clients.select('unique_id').distinct().count(),
         clients.groupBy('code_postal','unique_id').count().count(),
         clients.select('code_postal').distinct().count(),
         clients.groupBy('code_postal','ville').count().count(),
         clients.groupBy('code_postal','etat').count().count(),
         clients.groupBy('code_postal','ville','etat').count().count()))

clients.groupBy('unique_id').agg(countDistinct('id').alias('nb_cp')).where('nb_cp > 1').count()
clients.groupBy('id').agg(countDistinct('code_postal').alias('nb_cp')).where('nb_cp > 1').count()
clients.withColumn('control', expr('ville || etat || code_postal')).groupBy('code_postal','ville','etat').agg(countDistinct('control').alias('nb_cp')).where('nb_cp > 1').show()


print('enregistrements -- vendeurs             = %d\n\
id-code_postal,id                       = %d\t%d\n\
code_postal-code_postal,ville           = %d\t%d\n\
code_postal,etat-code_postal,ville,etat = %d\t%d'%(
         vendeurs.count(),
         vendeurs.select('id').distinct().count(),
         vendeurs.groupBy('code_postal','id').count().count(),
         vendeurs.select('code_postal').distinct().count(),
         vendeurs.groupBy('code_postal','ville').count().count(),
         vendeurs.groupBy('code_postal','etat').count().count(),
         vendeurs.groupBy('code_postal','ville','etat').count().count()))

vendeurs.groupBy('id').agg(countDistinct('code_postal').alias('nb_cp')).where('nb_cp > 1').count()
vendeurs.withColumn('control', expr('ville || etat || code_postal')).groupBy('code_postal').agg(countDistinct('control').alias('nb_cp')).where('nb_cp > 1').show()



print('enregistrements geolocation             = %d\n\
code_postal-code_postal,ville           = %d\t%d\n\
code_postal,etat-code_postal,ville,etat = %d\t%d'%(
      geolocation.count(),
      geolocation.select('code_postal').distinct().count(),
      geolocation.groupBy('code_postal','ville').count().count(),
      geolocation.groupBy('code_postal','etat').count().count(),
      geolocation.groupBy('code_postal','ville','etat').count().count()))

geolocation.select('code_postal').distinct().count()
geolocation.distinct().count()
cpEV       = Window.partitionBy('code_postal').orderBy('etat','ville')

geolocation.groupBy('code_postal')\
           .agg(
                min('latitude').alias('min_latitude'),
                max('latitude').alias('max_latitude'),
                min('longitude').alias('min_longitude'),
                max('longitude').alias('max_longitude'))\
           .orderBy('code_postal')\
           .join( geolocation.select('code_postal', 'ville', 'etat',
                    row_number().over(cpEV).alias('cpEV'))
                  .where('cpEV == 1'),'code_postal')\
           .show()

adresses = geolocation.groupBy('code_postal')\
           .agg(
                min('latitude').alias('min_latitude'),
                max('latitude').alias('max_latitude'),
                min('longitude').alias('min_longitude'),
                max('longitude').alias('max_longitude'))\
           .orderBy('code_postal')\
           .join( geolocation.select('code_postal', 'ville', 'etat',
                    row_number().over(cpEV).alias('cpEV'))
                  .where('cpEV == 1'),'code_postal')


adresses.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/adresses').save()
