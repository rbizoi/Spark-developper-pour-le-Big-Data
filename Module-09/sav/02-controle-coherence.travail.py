print("commandes enregistrements         = %d     %d"%(commandes.select("order_id").distinct().count(),commandes.count()))
print("details commandes enregistrements = %d     %d"%(details_commandes.count(),details_commandes.select("order_id").distinct().count()))
print("details commandes product_id      = %d       "%(details_commandes.select("product_id").distinct().count()))
print("produits enregistrements          = %d     %d"%(produits.select("product_id").distinct().count(),produits.count()))
print("vendeurs enregistrements          = %d     %d  zip_code %d"%(vendeurs.select("seller_id").distinct().count(),vendeurs.count(),vendeurs.select("seller_zip_code_prefix").distinct().count()))
print("paiements enregistrements         = %d     %d"%(paiements.select("order_id").distinct().count(),paiements.count()))
print("clients enregistrements           = %d     %d  zip_code %d"%(clients.select("customer_id").distinct().count(),clients.count(),clients.select("customer_zip_code_prefix").distinct().count()))
print("ventes enregistrements            = %d     %d"%(ventes.select("seller_id").distinct().count(),ventes.count()))
print("mql enregistrements               = %d     %d -- origin      %d"%(mql.select("mql_id").distinct().count(),mql.count(),mql.select("origin").distinct().count()))
print("adresses enregistrements          = %d     %d"%(adresses.select("code_postal").distinct().count(),adresses.count()))
print("categories enregistrements        = %d     %d"%(categories.select("product_category_name").distinct().count(),categories.select("product_category_name_english").distinct().count()))



print('\tcommandes                     = %d\t\t%d\n\
\tdetails commandes             = %d\t\t%d\tproduct_id\t%d\n\
\tproduits                      = %d\t\t%d\n\
\tvendeurs                      = %d\t\t%d\tzip_code\t%d\n\
\tpaiements                     = %d\t\t%d\n\
\tclients                       = %d\t\t%d\tzip_code\t%d\n\
\tventes                        = %d\t\t%d\n\
\tmql                           = %d\t\t%d\torigin\t\t%d\n\
\tadresses                      = %d\t\t%d\n\
\tcategories                    = %d\t\t%d'%(
    commandes.select("order_id").distinct().count(),
    commandes.count(),
    details_commandes.count(),
    details_commandes.select("order_id").distinct().count(),
    details_commandes.select("product_id").distinct().count(),
    produits.select("product_id").distinct().count(),
    produits.count(),
    vendeurs.select("seller_id").distinct().count(),
    vendeurs.count(),
    vendeurs.select("seller_zip_code_prefix").distinct().count(),
    paiements.select("order_id").distinct().count(),
    paiements.count(),
    clients.select("customer_id").distinct().count(),
    clients.count(),
    clients.select("customer_zip_code_prefix").distinct().count(),
    ventes.select("seller_id").distinct().count(),
    ventes.count(),
    mql.select("mql_id").distinct().count(),
    mql.count(),
    mql.select("origin").distinct().count(),
    adresses.select("code_postal").distinct().count(),
    adresses.count(),
    categories.select("product_category_name").distinct().count(),
    categories.select("product_category_name_english").distinct().count())
)

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import unicodedata

#-------------------------------------------------------------------------------------
# commandes.columns
#-------------------------------------------------------------------------------------
@udf("int")
def majOrderStatus(colonne):
    dictStrIntOS ={ 'shipped'    :0,
                    'canceled'   :1,
                    'approved'   :2,
                    'invoiced'   :3,
                    'created'    :4,
                    'delivered'  :5,
                    'unavailable':6,
                    'processing' :7}
    return int(dictStrIntOS[colonne])

@udf("string")
def affOrderStatus(colonne) :
    dictIntStrOS ={ 0:'expediée',
                    1:'annulée',
                    2:'validée',
                    3:'facturée',
                    4:'créée',
                    5:'livrée',
                    6:'indisponible',
                    7:'en cours'}
    return str(dictIntStrOS[colonne])

commandes.select('order_status')\
        .distinct()\
        .select('order_status',
                  majOrderStatus('order_status').alias('nouveau'))\
         .select('order_status','nouveau',
                  affOrderStatus('nouveau').alias('nouveau1'))\
         .show()
#-------------------------------------------------------------------------------------
# details_commandes.columns
#-------------------------------------------------------------------------------------
donnees0 = commandes.select('order_id',
                 'customer_id',
                 col('order_purchase_timestamp').alias('creee'),
                 majOrderStatus('order_status').alias('statut'),
                 unix_timestamp('order_purchase_timestamp').alias('creeeCalc'),
                 datediff('order_approved_at',
                          'order_purchase_timestamp').alias('validee'),
                 datediff('order_delivered_carrier_date',
                          'order_purchase_timestamp').alias('envoyee'),
                 datediff('order_delivered_customer_date',
                          'order_purchase_timestamp').alias('livree'),
                 datediff('order_estimated_delivery_date',
                          'order_purchase_timestamp').alias('estimation'))\
         .join(details_commandes, "order_id","left")\
         .select('order_id', 'order_item_id', 'product_id', 'seller_id', 'customer_id', 'creee', 'statut',
                 'creeeCalc', 'validee', 'envoyee', 'livree', 'estimation',
                 datediff('shipping_limit_date',
                          'creee').alias('limite'),
                 col('price').alias('prix'),
                 col('freight_value').alias('assurance'))\
         .cache()

donnees0.count()
donnees0.join(clients,'customer_id').show()

donnees0.where('product_id is null')\
       .groupBy('statut')\
       .agg(count('order_id').alias('decompte'))\
       .select(affOrderStatus('statut').alias('statut'),'decompte')\
       .show()


#-------------------------------------------------------------------------------------
# clients.columns
#-------------------------------------------------------------------------------------

clients.show()
clients.printSchema()
donnees0.select('customer_id').distinct().count()
clients0.select('customer_id').distinct().count()

donnees1 = donnees0.join(clients.select('customer_id',
                         col('customer_unique_id').alias('unique_id'),
                         col('customer_zip_code_prefix').alias('cp_client')),'customer_id')\
                  .drop('customer_id').cache()
donnees1.show()

#-------------------------------------------------------------------------------------
# produits.columns
#-------------------------------------------------------------------------------------
['product_id', 'product_category_name', 'product_name_lenght',
 'product_description_lenght', 'product_photos_qty', 'product_weight_g',
 'product_length_cm', 'product_height_cm', 'product_width_cm']

produits.show()
produits.printSchema()

catProduits = produits.select('product_category_name')\
                      .distinct()\
                      .orderBy('product_category_name')\
                      .toPandas()

dictStrIntCat ={ cat:i for i,cat
             in enumerate(catProduits.product_category_name.values)}

catProduits.fillna('non renseigné', inplace=True)

dictIntStrCat ={ i:cat for i,cat
             in enumerate(catProduits.product_category_name.values)}

@udf("int")
def majCategories(colonne):
    return int(dictStrIntCat[colonne])

@udf("string")
def affCategories(colonne) :
    return str(dictIntStrCat[colonne])

produits.select('product_category_name')\
        .distinct()\
        .select('product_category_name',
                  majCategories('product_category_name').alias('nouveau'))\
         .select('product_category_name','nouveau',
                  affCategories('nouveau').alias('nouveau1'))\
         .show()

produits.select('product_category_name')\
        .distinct()\
        .select('product_category_name',
                  majCategories('product_category_name').alias('nouveau'))\

produits.withColumn('product_category_name'            ,majCategories('product_category_name'))\
        .withColumnRenamed('product_category_name'     ,'categorie')\
        .withColumnRenamed('product_name_lenght'       ,'name_lenght')\
        .withColumnRenamed('product_description_lenght','description_lenght')\
        .withColumnRenamed('product_photos_qty'        ,'photos_qty')\
        .withColumnRenamed('product_weight_g'          ,'weight_g')\
        .withColumnRenamed('product_length_cm'         ,'length_cm')\
        .withColumnRenamed('product_height_cm'         ,'height_cm')\
        .withColumnRenamed('product_width_cm',         'width_cm')\
        .show()


donnees2 = donnees1.join(produits.withColumn('product_category_name'
                                            ,majCategories('product_category_name'))
                                .withColumnRenamed('product_category_name'
                                                    ,'categorie')
                                .withColumnRenamed('product_name_lenght'
                                                    ,'longueur_nom')
                                .withColumnRenamed('product_description_lenght'
                                                    ,'longueur_desc')
                                .withColumnRenamed('product_photos_qty'
                                                    ,'nb_photos')
                                .withColumnRenamed('product_weight_g'
                                                    ,'poids_g')
                                .withColumnRenamed('product_length_cm'
                                                    ,'longueur_cm')
                                .withColumnRenamed('product_height_cm'
                                                    ,'hauteur_cm')
                                .withColumnRenamed('product_width_cm'
                                                    ,'largeur_cm'),'product_id','left')
                  #.drop('product_id').cache()
donnees2.show()
donnees2.count()

#-------------------------------------------------------------------------------------
# paiements.columns
#-------------------------------------------------------------------------------------
#['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value']
paiements.show()

fenMens = Window.partitionBy('order_id')
paiements.select('order_id', 'payment_sequential','payment_type',
                 'payment_installments','payment_value',
                 count('payment_type').over(fenMens).alias('sequence'),
                 min('payment_value').over(fenMens).alias('montant_min'),
                 max('payment_value').over(fenMens).alias('montant_mAX'),
                 round(avg('payment_value').over(fenMens),2).alias('montant_avg'))\
          .where('sequence > 1')\
          .orderBy('order_id', 'payment_sequential')\
          .show(1000,truncate=False)

paiements1 = paiements.select('order_id',
                 'payment_sequential',
                 'payment_type',
                 col('payment_installments').alias('versements'),
                 col('payment_value').alias('montant'),
                 count('payment_type').over(fenMens).alias('sequence'),
                 min('payment_value').over(fenMens).alias('montant_min'),
                 max('payment_value').over(fenMens).alias('montant_max'),
                 round(avg('payment_value').over(fenMens),2).alias('montant_avg'))\
         .groupBy('order_id','sequence','montant_min','montant_max','montant_avg')\
         .pivot('payment_type')\
         .agg(
              sum('versements'),
              avg('montant')
              )

import re
motif1 = re.compile('^([a-z_]+)(\(CAST\(|\()([a-z]+)\s(AS BIGINT\)\))$')
motif2 = re.compile('^([a-z_]+)\(([a-z]+)\)$')
lnoms = paiements1.columns
lnoms = [ motif2.sub(r'\1_\2',motif1.sub(r'\1_\3',x))for x in lnoms]
paiementsNew = paiements1.toDF(*lnoms)
#.fillna(0)

donnees3 = donnees2.join(paiementsNew,'order_id','left')
donnees3.show()
donnees3.select('order_id').distinct().count()

#-------------------------------------------------------------------------------------
# descriptions_commandes.columns
#-------------------------------------------------------------------------------------
#['review_id', 'order_id', 'review_score', 'review_comment_title',
# 'review_comment_message', 'review_creation_date',
# 'review_answer_timestamp', 'creation_com', 'reponse_com']
descriptions_commandes.count()
descriptions_commandes.select('order_id').distinct().count()
descriptions_commandes.groupBy('order_id').agg(countDistinct('review_id').alias('nb')).select('nb').max('nb').show()


descriptions_commandes.groupBy('order_id')\
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
                            )\
                      .orderBy(desc('nb_comentaires'))\
                      .show(500,truncate=False)

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
                            ),'order_id')
donnees4.count()
donnees4.select('order_id').distinct().count()
donnees4.show()

#-------------------------------------------------------------------------------------
# vendeurs.columns
#-------------------------------------------------------------------------------------
vendeurs.show()
vendeurs.printSchema()

donnees4.select('seller_id').distinct().count()
donnees4.select('seller_id').distinct().orderBy().show()

vendeurs.select('seller_id',col('seller_zip_code_prefix').alias('cp_vendeur')).show()

donnees5 = donnees4.join( vendeurs.select('seller_id',
                          col('seller_zip_code_prefix').alias('cp_vendeur')),
                          'seller_id','left')\
                   .cache()
donnees5.show()




donnees5.write.mode('overwrite').format('parquet')\
        .option('path','/user/spark/donnees/brazilian_e-commerce/parquet/brazilian_ecommerce').save()

#-------------------------------------------------------------------------------------
# mql.columns
#-------------------------------------------------------------------------------------
#['mql_id', 'first_contact_date', 'landing_page_id', 'origin']
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

mql.select('origin')\
        .distinct()\
        .select('origin',
                  majOrigins('origin').alias('nouveau'))\
         .select('origin','nouveau',
                  affOrigins('nouveau').alias('nouveau1'))\
         .show()

pageId = mql.select('landing_page_id')\
                      .distinct()\
                      .orderBy('landing_page_id')\
                      .toPandas()

dictStrIntPageId ={ cat:i for i,cat
             in enumerate(pageId.landing_page_id.values)}

@udf("int")
def majPageId(colonne):
    return int(dictStrIntPageId[colonne])

mql.select('landing_page_id')\
        .distinct()\
        .select('landing_page_id',
                  majPageId('landing_page_id').alias('nouveau'))\
        .show()

#-------------------------------------------------------------------------------------
# ventes.columns
#-------------------------------------------------------------------------------------
#[ 'mql_id', 'seller_id', 'sdr_id', 'sr_id', 'won_date', 'business_segment', 'lead_type',
# 'lead_behaviour_profile', 'has_company', 'has_gtin', 'average_stock', 'business_type',
# 'declared_product_catalog_size', 'declared_monthly_revenue']

ventes.show()
ventes.printSchema()

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

ventes.select('business_segment')\
        .distinct()\
        .select('business_segment',
                  majSegments('business_segment').alias('nouveau'))\
         .select('business_segment','nouveau',
                  affSegments('nouveau').alias('nouveau1'))\
         .show()


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

ventes.select('lead_type')\
        .distinct()\
        .select('lead_type',
                  majProspects('lead_type').alias('nouveau'))\
         .select('lead_type','nouveau',
                  affProspects('nouveau').alias('nouveau1'))\
         .show()


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

ventes.select('lead_behaviour_profile')\
        .distinct()\
        .select('lead_behaviour_profile',
                  majComportements('lead_behaviour_profile').alias('nouveau'))\
         .select('lead_behaviour_profile','nouveau',
                  affComportements('nouveau').alias('nouveau1'))\
         .show()

#[ 'mql_id', 'seller_id', 'sdr_id', 'sr_id', 'won_date', 'business_segment', 'lead_type',
# 'lead_behaviour_profile', 'has_company', 'has_gtin', 'average_stock', 'business_type',
# 'declared_product_catalog_size', 'declared_monthly_revenue']
ventes.select('seller_id', 'won_date',
               majSegments('business_segment').alias('segment'),
               majProspects('lead_type').alias('prospect'),
               majComportements('lead_behaviour_profile').alias('comportement'),
               'has_company',
               'has_gtin',
               'average_stock',
               'declared_product_catalog_size', 'declared_monthly_revenue')\
       .show()

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
