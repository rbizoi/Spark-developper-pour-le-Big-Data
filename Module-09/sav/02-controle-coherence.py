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
#-------------------------------------------------------------------------------------
# clients.columns
#-------------------------------------------------------------------------------------
donnees1 = donnees0.join(clients.select('customer_id',
                         col('customer_unique_id').alias('unique_id'),
                         col('customer_zip_code_prefix').alias('cp_client')),'customer_id')\
                  .drop('customer_id').cache()
#-------------------------------------------------------------------------------------
# produits.columns
#-------------------------------------------------------------------------------------
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
                            ),'order_id')
#-------------------------------------------------------------------------------------
# vendeurs.columns
#-------------------------------------------------------------------------------------
donnees5 = donnees4.join( vendeurs.select('seller_id',
                          col('seller_zip_code_prefix').alias('cp_vendeur')),
                          'seller_id','left')\
                   .cache()
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
