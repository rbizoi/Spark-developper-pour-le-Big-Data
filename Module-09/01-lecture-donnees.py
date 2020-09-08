from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.ml.feature import QuantileDiscretizer
import re
import unicodedata

from pyspark.sql import SparkSession
spark = SparkSession.builder.\
        appName("Brazilian_E-Commerce").getOrCreate()

spark.catalog.clearCache()
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

schema = "customer_id  string, customer_unique_id  string, customer_zip_code_prefix  integer, customer_city  string, customer_state  string"
clients  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_customers_dataset.csv')

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


schema = "mql_id  string, first_contact_date  date, landing_page_id  string, origin  string"
mql  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_marketing_qualified_leads_dataset.csv')

schema = "order_id  string, customer_id  string, order_status  string, order_purchase_timestamp  timestamp, order_approved_at  timestamp, order_delivered_carrier_date  timestamp, order_delivered_customer_date  timestamp, order_estimated_delivery_date  timestamp"
commandes  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_orders_dataset.csv')

schema = "order_id  string, order_item_id  integer, product_id  string, seller_id  string, shipping_limit_date  timestamp, price  double, freight_value  double"
details_commandes  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_order_items_dataset.csv')

schema = "order_id  string, payment_sequential  integer, payment_type  string, payment_installments  integer, payment_value  double"
paiements  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_order_payments_dataset.csv')

schema = "review_id  string, order_id  string, review_score  int, review_comment_title  string, review_comment_message  string, review_creation_date  timestamp, review_answer_timestamp  timestamp"
descriptions_commandes  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_order_reviews_dataset.csv')

descriptions_commandes.join(commandes,'order_id')\
        .select('review_id', 'order_id', 'review_score', 'review_comment_title',
                'review_comment_message', 'review_creation_date',
                'review_answer_timestamp',
                 datediff('review_creation_date','order_purchase_timestamp').alias('creation_com'),
                 datediff('review_answer_timestamp','review_creation_date').alias('reponse_com')
                ).write.mode('overwrite')\
        .format('parquet')\
        .option('path','/user/spark/donnees/brazilian_e-commerce/parquet/descriptions_commandes').save()

schema = "product_id  string, product_category_name  string, product_name_lenght  integer, product_description_lenght  integer, product_photos_qty  integer, product_weight_g  integer, product_length_cm  integer, product_height_cm  integer, product_width_cm  integer"
produits  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_products_dataset.csv')

schema = "seller_id  string, seller_zip_code_prefix  integer, seller_city  string, seller_state  string"
vendeurs  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/olist_sellers_dataset.csv')

schema = "product_category_name  string, product_category_name_english  string"
categories  = spark.read.format('csv')\
          .option('header','true')\
          .option('nullValue','mq')\
          .option('mergeSchema', 'true')\
          .schema(schema)\
          .load('donnees/brazilian_e-commerce/product_category_name_translation.csv')



#-------------------------------------------------------------------------------------
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
#-------------------------------------------------------------------------------------
# commandes.columns
#-------------------------------------------------------------------------------------
#@udf("int")
#def majOrderStatus(colonne):
#    dictStrIntOS ={ 'shipped'    :0,
#                    'canceled'   :1,
#                    'approved'   :2,
#                    'invoiced'   :3,
#                    'created'    :4,
#                    'delivered'  :5,
#                    'unavailable':6,
#                    'processing' :7}
#    return int(dictStrIntOS[colonne])
#
#
#@udf("string")
#def affOrderStatus(colonne) :
#    dictIntStrOS ={ 0:'expediée'    ,
#                    1:'annulée'     ,
#                    2:'validée'     ,
#                    3:'facturée'    ,
#                    4:'créée'       ,
#                    5:'livrée'      ,
#                    6:'indisponible',
#                    7:'en cours'
#            }
#    return str(dictIntStrOS[colonne])
@udf("string")
def majOrderStatus(colonne):
    dictStrIntOS ={ 'shipped'    :'expediée'    ,
                    'canceled'   :'annulée'     ,
                    'approved'   :'validée'     ,
                    'invoiced'   :'facturée'    ,
                    'created'    :'créée'       ,
                    'delivered'  :'livrée'      ,
                    'unavailable':'indisponible',
                    'processing' :'en cours'
                    }
    return str(dictStrIntOS[colonne])

#-------------------------------------------------------------------------------------
# details_commandes.columns
#-------------------------------------------------------------------------------------
@udf("string")
def majJoursSemaine(colonne) :
    dictIntStrJours ={ 2:'lundi',
                    3:'mardi',
                    4:'mercredi',
                    5:'jeudi',
                    6:'vendredi',
                    7:'samedi',
                    1:'dimanche'}
    return str(dictIntStrJours[colonne])


@udf("string")
def majJoursMois(colonne) :
    dictIntStrMois ={1:'janvier',
                        2:'février',
                        3:'mars',
                        4:'avril',
                        5:'mai',
                        6:'juin',
                        7:'juillet',
                        8:'août',
                        9:'septembre',
                        10:'octobre',
                        11:'novembre',
                        12:'décembre'}
    return str(dictIntStrMois[colonne])


donnees0 = commandes.select('order_id',
                 'customer_id',
                 col('order_purchase_timestamp').alias('creee'),
                 majOrderStatus('order_status').alias('statut'),
                 year('order_purchase_timestamp').alias('annee'),
                 month('order_purchase_timestamp').alias('mois12'),
                 majJoursMois(month('order_purchase_timestamp')).alias('mois12s'),
                 (year('order_purchase_timestamp')*100 + month('order_purchase_timestamp')).alias('mois'),
                 (year('order_purchase_timestamp')*100 + weekofyear('order_purchase_timestamp')).alias('semaine'),
                 weekofyear('order_purchase_timestamp').alias('semaine53'),
                 dayofyear('order_purchase_timestamp').alias('jour365'),
                 ( year('order_purchase_timestamp')*10000 +
                   month('order_purchase_timestamp')*100 +
                   dayofmonth('order_purchase_timestamp')).alias('jour'),
                   dayofweek('order_purchase_timestamp').alias('jour7'),
                   majJoursSemaine(dayofweek('order_purchase_timestamp')).alias('jour7s'),
                 hour('order_purchase_timestamp').alias('heure24'),
                 datediff('order_approved_at',
                          'order_purchase_timestamp').alias('validee'),
                 datediff('order_delivered_carrier_date',
                          'order_purchase_timestamp').alias('envoyee'),
                 datediff('order_delivered_customer_date',
                          'order_purchase_timestamp').alias('livree'),
                 datediff('order_estimated_delivery_date',
                          'order_purchase_timestamp').alias('estimation')).fillna(0)\
         .join(details_commandes, "order_id","left")\
         .select('order_id', 'product_id', 'seller_id', 'customer_id', 'creee', 'statut', 'annee',
                 'mois12', 'mois12s', 'mois', 'semaine', 'semaine53', 'jour365',
                 'jour', 'jour7', 'jour7s', 'heure24', 'validee',
                 'envoyee', 'livree', 'estimation',
                 datediff('shipping_limit_date',
                          'creee').alias('limite'),
                 col('price').alias('prix'),
                 col('freight_value').alias('assurance'))\
         .cache()



discretizer3h = QuantileDiscretizer(numBuckets=8, inputCol="heure24", outputCol="periode3H")
discretizer6h = QuantileDiscretizer(numBuckets=4, inputCol="heure24", outputCol="periode6H")

donnees01 = discretizer3h.fit(donnees0).transform(donnees0)
donnees02 = discretizer6h.fit(donnees01).transform(donnees01)

#-------------------------------------------------------------------------------------
# clients.columns
#-------------------------------------------------------------------------------------
donnees1 = donnees02.join(clients.select('customer_id',
                         col('customer_unique_id').alias('client_uid'),
                         col('customer_zip_code_prefix').alias('cp_client')),'customer_id')\
                  .drop('customer_id').cache()
#-------------------------------------------------------------------------------------
# produits.columns
#-------------------------------------------------------------------------------------
categories = spark.sql("select * from parquet.`/user/spark/donnees/brazilian_e-commerce/parquet/categories`")\
                  .join(produits.select('product_category_name').distinct(),'product_category_name','right')\
                  .toPandas()

categories.product_category_name_english [(categories.product_category_name_english.isnull())&
           (categories.product_category_name == 'pc_gamer')]  = "pc_gamer"

categories.product_category_name_english [(categories.product_category_name_english.isnull())&
           (categories.product_category_name == 'portateis_cozinha_e_preparadores_de_alimentos')]  = "portable_kitchen_and_food_preparers"

dictStrIntCat ={ cat:i for i,cat
             in enumerate(categories.sort_values('product_category_name').product_category_name.values)}

dictIntStrCat ={ p:e for p,e
             in zip(categories.product_category_name.values,
                    categories.product_category_name_english.values)}

@udf("int")
def majCategories(colonne):
    return int(dictStrIntCat[colonne])

@udf("string")
def affCategories(colonne) :
    ret = str(dictIntStrCat[colonne])
    if ret == 'None' :
        return 'not documented'
    else :
        return str(dictIntStrCat[colonne])


produits01 = produits.withColumn('categorie',majCategories('product_category_name'))  \
                    .withColumn('categorieEng',affCategories('product_category_name'))\
                    .withColumnRenamed('product_category_name','categoriePor')        \
                    .withColumnRenamed('product_name_lenght','longueur_nom')          \
                    .withColumnRenamed('product_description_lenght','longueur_desc')  \
                    .withColumnRenamed('product_photos_qty','nb_photos')              \
                    .withColumnRenamed('product_weight_g','poids_g')                  \
                    .withColumnRenamed('product_length_cm','longueur_cm')             \
                    .withColumnRenamed('product_height_cm','hauteur_cm')              \
                    .withColumnRenamed('product_width_cm','largeur_cm')               \
                    .na.fill('not documented',['categoriePor'])                       \
                    .na.fill(0,['longueur_nom', 'longueur_desc', 'nb_photos', 'poids_g', 'longueur_cm', 'hauteur_cm', 'largeur_cm'])


donnees2 = donnees1.join( produits01,'product_id','left')

#-------------------------------------------------------------------------------------
# paiements.columns
#-------------------------------------------------------------------------------------
fenMens = Window.partitionBy('order_id')
paiements1 = paiements.select('order_id',
                 'payment_sequential',
                 'payment_type',
                 col('payment_installments').alias('versements'),
                 col('payment_value').alias('montant'),
                 count('payment_type').over(fenMens).alias('sequence'),
                 min('payment_value').over(fenMens).alias('montant_min'),
                 max('payment_value').over(fenMens).alias('montant_max'),
                 round(sum('payment_value').over(fenMens),2).alias('montant_sum'),
                 round(avg('payment_value').over(fenMens),2).alias('montant_avg'))\
         .groupBy('order_id','sequence','montant_min',
                  'montant_max','montant_sum','montant_avg')\
         .pivot('payment_type')\
         .agg(
              sum('versements'),
              avg('montant')
              ).fillna(0)

lnoms = paiements1.columns
remplacement = {'boleto':'es',
                'credit_card':'cc',
                'debit_card':'cb',
                'voucher':'ba',
                'not_defined':'nr',
                'versements':'vers',
                'montant':'mont'}

motif1 = re.compile('^([a-z_]+)(\(CAST\(|\()([a-z]+)\s(AS BIGINT\)\))$')
motif2 = re.compile('^([a-z_]+)\(([a-z]+)\)$')
lnoms = [ motif2.sub(r'\1_\2',motif1.sub(r'\1_\3',x))for x in lnoms]

def replace_all(chaine, dic_rempl):
    for i in dic_rempl:
        chaine = chaine.replace(i, dic_rempl[i])
    return chaine

lnoms = [replace_all(x,remplacement)   for x in lnoms]
paiementsNew = paiements1.toDF(*lnoms)

donnees3 = donnees2.join(paiementsNew.drop('nr_sum_vers','nr_avg_mont'),'order_id','left')
#-------------------------------------------------------------------------------------
# descriptions_commandes.columns
#-------------------------------------------------------------------------------------
donnees4 = donnees3.join(descriptions_commandes.groupBy('order_id')\
                      .agg(
                            count('review_id').alias('nb_comentaires'),
                            min('review_score').alias('note_min'),
                            max('review_score').alias('note_max'),
                            round(avg('review_score'),2).alias('note_avg'),
                            min('creation_com').alias('create_min'),
                            max('creation_com').alias('create_max'),
                            round(avg('creation_com'),2).alias('create_avg'),
                            min('reponse_com').alias('reponse_min'),
                            max('reponse_com').alias('reponse_max'),
                            round(avg('reponse_com'),2).alias('reponse_avg')
                            ),'order_id').fillna(0)
#-------------------------------------------------------------------------------------
# vendeurs.columns
#-------------------------------------------------------------------------------------
donnees5 = donnees4.join( vendeurs.select('seller_id',
                          col('seller_zip_code_prefix').alias('cp_vendeur')),
                          'seller_id','left')\
                   .where('seller_id is not null')\
                   .cache()


commandeId = donnees5.select('order_id')\
                      .distinct()\
                      .orderBy('order_id')\
                      .toPandas()

dictStrIntCommId ={ cat:i for i,cat
             in enumerate(commandeId.order_id.values)}

@udf("int")
def majCommandeId(colonne):
    return int(dictStrIntCommId[colonne])


vendeurId = donnees5.select('seller_id')\
                      .distinct()\
                      .orderBy('seller_id')\
                      .toPandas()

dictStrIntVendId ={ cat:i for i,cat
             in enumerate(vendeurId.seller_id.values)}

@udf("int")
def majVendeurId(colonne):
    return int(dictStrIntVendId[colonne])


produitId = donnees5.select('product_id')\
                      .distinct()\
                      .orderBy('product_id')\
                      .toPandas()

dictStrIntProdId ={ cat:i for i,cat
             in enumerate(produitId.product_id.values)}

@udf("int")
def majProduitId(colonne):
    return int(dictStrIntProdId[colonne])

clientUId = donnees5.select('client_uid')\
                      .distinct()\
                      .orderBy('client_uid')\
                      .toPandas()

dictStrIntCliUId ={ cat:i for i,cat
             in enumerate(clientUId.client_uid.values)}

@udf("int")
def majClientUId(colonne):
    return int(dictStrIntCliUId[colonne])


donnees5.withColumn('order_id', majCommandeId('order_id'))\
        .withColumn('seller_id', majVendeurId('seller_id'))\
        .withColumn('product_id', majProduitId('product_id'))\
        .withColumn('client_uid', majClientUId('client_uid'))\
        .write.mode('overwrite').format('parquet')\
        .option('path','/user/spark/donnees/brazilian_e-commerce/parquet/brazilian_ecommerce').save()

donnees6 = donnees5.withColumn('order_id', majCommandeId('order_id'))\
                   .withColumn('seller_id', majVendeurId('seller_id'))\
                   .withColumn('product_id', majProduitId('product_id'))\
                   .withColumn('client_uid', majClientUId('client_uid'))\
                   .join(adresses.drop('cpEV').withColumnRenamed('code_postal','cp_client'),
                         'cp_client','left')

lnoms = donnees6.columns
remplacement = {'min_latitude'  :'cli_min_lat',
                'max_latitude'  :'cli_max_lat',
                'min_longitude' :'cli_min_lng',
                'max_longitude' :'cli_max_lng',
                'ville'         :'cli_ville'  ,
                'etat'          :'cli_etat'     }

lnoms = [replace_all(x,remplacement)   for x in lnoms]
donnees7 = donnees6.toDF(*lnoms)

donnees8 = donnees7.join(adresses.drop('cpEV').withColumnRenamed('code_postal','cp_vendeur'),'cp_vendeur','left')
lnoms = donnees8.columns
remplacement = {'min_latitude'  :'vnd_min_lat',
                'max_latitude'  :'vnd_max_lat',
                'min_longitude' :'vnd_min_lng',
                'max_longitude' :'vnd_max_lng',
                'ville'         :'vnd_ville'  ,
                'etat'          :'vnd_etat'   ,
                'cli_vnd_ville' : 'cli_ville',
                'cli_vnd_etat'  : 'cli_etat'}

lnoms = [replace_all(x,remplacement)   for x in lnoms]
donnees9 = donnees8.toDF(*lnoms)

donnees9.write.mode('overwrite').format('parquet')\
        .option('path','/user/spark/donnees/brazilian_e-commerce/parquet/brazilian_ecommerce_adresses').save()
#-------------------------------------------------------------------------------------
# mql.columns
#-------------------------------------------------------------------------------------
typeOrigins = mql.select('origin')\
                      .distinct()\
                      .orderBy('origin')\
                      .toPandas()

dictStrIntOrig ={ cat:i for i,cat
             in enumerate(typeOrigins.origin.values)}
dictStrIntOrig['unknown']=0

typeOrigins.fillna('unknown', inplace=True)

dictIntStrOrig ={ i:cat for i,cat
             in enumerate(typeOrigins.origin.values)}
dictIntStrOrig[0] = 'non renseigné'

@udf("int")
def majOrigins(colonne):
    return int(dictStrIntOrig[colonne])


@udf("string")
def affOrigins(colonne) :
    return str(dictIntStrOrig[colonne])


pageId = mql.select('landing_page_id')\
                      .distinct()\
                      .orderBy('landing_page_id')\
                      .toPandas()

dictStrIntPageId ={ cat:i for i,cat
             in enumerate(pageId.landing_page_id.values)}

@udf("int")
def majPageId(colonne):
    return int(dictStrIntPageId[colonne])


#-------------------------------------------------------------------------------------
# ventes.columns
#-------------------------------------------------------------------------------------
typeSegments = ventes.select('business_segment')\
                      .distinct()\
                      .orderBy('business_segment')\
                      .toPandas()

dictStrIntSegm ={ cat:i for i,cat
             in enumerate(typeSegments.business_segment.values)}

typeSegments.fillna('non renseigné', inplace=True)

dictIntStrSegm ={ i:cat for i,cat
             in enumerate(typeSegments.business_segment.values)}

@udf("int")
def majSegments(colonne):
    return int(dictStrIntSegm[colonne])


@udf("string")
def affSegments(colonne) :
    return str(dictIntStrSegm[colonne])


typeProspects = ventes.select('lead_type')\
                      .distinct()\
                      .orderBy('lead_type')\
                      .toPandas()

dictStrIntProsp ={ cat:i for i,cat
             in enumerate(typeProspects.lead_type.values)}

typeProspects.fillna('non renseigné', inplace=True)

dictIntStrProsp ={ i:cat for i,cat
             in enumerate(typeProspects.lead_type.values)}

@udf("int")
def majProspects(colonne):
    return int(dictStrIntProsp[colonne])


@udf("string")
def affProspects(colonne) :
    return str(dictIntStrProsp[colonne])


typeComportements = ventes.select('lead_behaviour_profile')\
                      .distinct()\
                      .orderBy('lead_behaviour_profile')\
                      .toPandas()

dictStrIntComp ={ cat:i for i,cat
             in enumerate(typeComportements.lead_behaviour_profile.values)}

typeComportements.fillna('non renseigné', inplace=True)

dictIntStrComp ={ i:cat for i,cat
             in enumerate(typeComportements.lead_behaviour_profile.values)}

@udf("int")
def majComportements(colonne):
    return int(dictStrIntComp[colonne])


@udf("string")
def affComportements(colonne) :
    return str(dictIntStrComp[colonne])


ventes.select('seller_id',
              'mql_id',
              'won_date',
               majSegments('business_segment').alias('segment'),
               majProspects('lead_type').alias('prospect'),
               majComportements('lead_behaviour_profile').alias('comportement'),
               'has_company',
               'has_gtin',
               'average_stock',
               'declared_product_catalog_size', 'declared_monthly_revenue')\
       .join(mql.select('mql_id','first_contact_date',
                   majOrigins('origin').alias('origine'),
                   majPageId('landing_page_id').alias('pageId')),'mql_id')\
       .write.mode('overwrite').format('parquet')\
       .option('path','/user/spark/donnees/brazilian_e-commerce/parquet/ventes_mql').save()
#-------------------------------------------------------------------------------------
