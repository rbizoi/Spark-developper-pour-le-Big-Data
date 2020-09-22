from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import unicodedata

@udf("string")
def nettoyer(colonne):
    nk = unicodedata.normalize('NFKD', colonne)
    return str(nk.encode('ASCII', 'ignore').decode('ASCII'))


schema = "mql_id  string, seller_id  string, sdr_id  string, sr_id  string, won_date  timestamp, business_segment  string, lead_type  string, lead_behaviour_profile  string, has_company  boolean, has_gtin  boolean, average_stock  string, business_type  string, declared_product_catalog_size  double, declared_monthly_revenue  double"
ventes  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_closed_deals_dataset.csv')
ventes.printSchema()
ventes.show(5)
ventes.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/ventes').save()

schema = "customer_id  string, customer_unique_id  string, customer_zip_code_prefix  integer, customer_city  string, customer_state  string"
clients  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_customers_dataset.csv')
clients.printSchema()
clients.show(5)
clients.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/clients').save()

schema = "geolocation_zip_code_prefix  integer, geolocation_lat  double, geolocation_lng  double, geolocation_city  string, geolocation_state  string"
geolocation  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_geolocation_dataset.csv')\
                    .select(col('geolocation_zip_code_prefix').alias('code_postal'),
                                nettoyer('geolocation_city').alias('ville'),
                                nettoyer('geolocation_state').alias('etat'),
                                col('geolocation_lat').alias('latitude'),
                                col('geolocation_lng').alias('longitude'))

cpEV       = Window.partitionBy('code_postal').orderBy('etat','ville')

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

schema = "mql_id  string, first_contact_date  date, landing_page_id  string, origin  string"
mql  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_marketing_qualified_leads_dataset.csv')
mql.printSchema()
mql.show(5)
mql.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/mql').save()

schema = "order_id  string, customer_id  string, order_status  string, order_purchase_timestamp  timestamp, order_approved_at  timestamp, order_delivered_carrier_date  timestamp, order_delivered_customer_date  timestamp, order_estimated_delivery_date  timestamp"
commandes  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_orders_dataset.csv')
commandes.printSchema()
commandes.show(5)
commandes.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/commandes').save()

schema = "order_id  string, order_item_id  integer, product_id  string, seller_id  string, shipping_limit_date  timestamp, price  double, freight_value  double"
details_commandes  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_order_items_dataset.csv')
details_commandes.printSchema()
details_commandes.show(5)
details_commandes.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/details_commandes').save()

schema = "order_id  string, payment_sequential  integer, payment_type  string, payment_installments  integer, payment_value  double"
paiements  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_order_payments_dataset.csv')
paiements.printSchema()
paiements.show(5)
paiements.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/paiements').save()

schema = "review_id  string, order_id  string, review_score  int, review_comment_title  string, review_comment_message  string, review_creation_date  timestamp, review_answer_timestamp  timestamp"
descriptions_commandes  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_order_reviews_dataset.csv')
descriptions_commandes.printSchema()
descriptions_commandes.show(5)
descriptions_commandes.join(commandes,'order_id')\
        .select('review_id', 'order_id', 'review_score', 'review_comment_title',
                'review_comment_message', 'review_creation_date',
                'review_answer_timestamp',
                 datediff('review_creation_date','order_purchase_timestamp').alias('creation_com'),
                 datediff('review_answer_timestamp','review_creation_date').alias('reponse_com')
                ).write.mode('overwrite')\
        .format('parquet')\
        .option('path','/user/spark/donnees/brazilian_e-commerce/parquet/descriptions_commandes').save()
#descriptions_commandes.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/descriptions_commandes').save()

schema = "product_id  string, product_category_name  string, product_name_lenght  integer, product_description_lenght  integer, product_photos_qty  integer, product_weight_g  integer, product_length_cm  integer, product_height_cm  integer, product_width_cm  integer"
produits  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_products_dataset.csv')
produits.printSchema()
produits.show(5)
produits.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/produits').save()

schema = "seller_id  string, seller_zip_code_prefix  integer, seller_city  string, seller_state  string"
vendeurs  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_sellers_dataset.csv')
vendeurs.printSchema()
vendeurs.show(5)
vendeurs.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/vendeurs').save()

schema = "product_category_name  string, product_category_name_english  string"
categories  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/product_category_name_translation.csv')
categories.printSchema()
categories.show(5)
categories.write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/categories').save()


spark.catalog.clearCache()
ventes                 = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/ventes`").cache()
clients                = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/clients`").cache()
adresses               = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/adresses`").cache()
mql                    = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/mql`").cache()
commandes              = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/commandes`").cache()
details_commandes      = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/details_commandes`").cache()
paiements              = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/paiements`").cache()
descriptions_commandes = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/descriptions_commandes`").cache()
produits               = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/produits`").cache()
vendeurs               = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/vendeurs`").cache()
categories             = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/categories`").cache()


ventes.show()
clients.show()
adresses.show()
mql.show()
commandes.show()
details_commandes.show()
paiements.show()
descriptions_commandes.show()
produits.show()
vendeurs.show()
categories.show()

ventes.count()
clients.count()
adresses.count()
mql.count()
commandes.count()
details_commandes.count()
paiements.count()
descriptions_commandes.count()
produits.count()
vendeurs.count()
categories.count()


#-------------------------------------------------------------------------------------
# commandes.columns
#-------------------------------------------------------------------------------------
#[ 'order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
#  'order_approved_at', 'order_delivered_carrier_date',
# 'order_delivered_customer_date', 'order_estimated_delivery_date']

#-------------------------------------------------------------------------------------
# details_commandes.columns
#-------------------------------------------------------------------------------------
#[ 'order_id', 'order_item_id', 'product_id', 'seller_id',
#  'shipping_limit_date', 'price', 'freight_value']
#-------------------------------------------------------------------------------------
# adresses.columns
#-------------------------------------------------------------------------------------
#['code_postal', 'min_latitude', 'max_latitude', 'min_longitude', 'max_longitude',
# 'ville', 'etat', 'cpEV']
#-------------------------------------------------------------------------------------
# clients.columns
#-------------------------------------------------------------------------------------
#['customer_id', 'customer_unique_id', 'customer_zip_code_prefix',
# 'customer_city', 'customer_state']
#-------------------------------------------------------------------------------------
# paiements.columns
#-------------------------------------------------------------------------------------
#['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value']
#-------------------------------------------------------------------------------------
# descriptions_commandes.columns
#-------------------------------------------------------------------------------------
#['review_id', 'order_id', 'review_score', 'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp']
#-------------------------------------------------------------------------------------
# produits.columns
#-------------------------------------------------------------------------------------
#['product_id', 'product_category_name', 'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
#-------------------------------------------------------------------------------------
# vendeurs.columns
#-------------------------------------------------------------------------------------
#['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']
#-------------------------------------------------------------------------------------
# categories.columns
#-------------------------------------------------------------------------------------
#['product_category_name', 'product_category_name_english']
#-------------------------------------------------------------------------------------
# ventes.columns
#-------------------------------------------------------------------------------------
#[ 'mql_id', 'seller_id', 'sdr_id', 'sr_id', 'won_date', 'business_segment', 'lead_type',
# 'lead_behaviour_profile', 'has_company', 'has_gtin', 'average_stock', 'business_type',
# 'declared_product_catalog_size', 'declared_monthly_revenue']
#-------------------------------------------------------------------------------------
# mql.columns
#-------------------------------------------------------------------------------------
#['mql_id', 'first_contact_date', 'landing_page_id', 'origin']


commandes.join(details_commandes,'order_id').show()


.join(commandes,'order_id')\
vendeurs.join(details_commandes,'seller_id')\
        .join(produits,'product_id')\
        .join(categories,'product_category_name')\
        .join(paiements,'order_id')\
        .join(clients,'customer_id')\
        .join(descriptions_commandes,'order_id')\
        .join(ventes,'seller_id')\
        .join(mql,'mql_id')\
        .join(geolocation.select(  col('geolocation_zip_code_prefix').alias('customer_zip_code_prefix'),
                                   col('geolocation_lat').alias('customer_lat'),
                                   col('geolocation_lng').alias('customer_lng'))
                                   ,'customer_zip_code_prefix')\
        .join(geolocation.select(  col('geolocation_zip_code_prefix').alias('seller_zip_code_prefix'),
                                   col('geolocation_lat'  ).alias('seller_lat'),
                                   col('geolocation_lng'  ).alias('seller_lng'))
                                   ,'seller_zip_code_prefix')\
           .show(1)

vendeurs.join(details_commandes,'seller_id')\
        .join(commandes,'order_id')\
        .join(produits,'product_id')\
        .join(categories,'product_category_name')\
        .join(paiements,'order_id')\
        .join(clients,'customer_id')\
        .join(descriptions_commandes,'order_id')\
        .join(ventes,'seller_id')\
        .join(mql,'mql_id')\
        .join(geolocation.select(  col('geolocation_zip_code_prefix').alias('customer_zip_code_prefix'),
                                   col('geolocation_lat').alias('customer_lat'),
                                   col('geolocation_lng').alias('customer_lng'))
                                   ,'customer_zip_code_prefix')\
        .join(geolocation.select(  col('geolocation_zip_code_prefix').alias('seller_zip_code_prefix'),
                                   col('geolocation_lat'  ).alias('seller_lat'),
                                   col('geolocation_lng'  ).alias('seller_lng'))
                                   ,'seller_zip_code_prefix')\
        .write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/jeuxcomplet').save()



spark.conf.set("spark.sql.shuffle.partitions",1)
spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/jeuxcomplet`").write.mode('overwrite').format('parquet').option('path','/user/spark/donnees/brazilian_e-commerce/parquet/brasilianEC').save()
