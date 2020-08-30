import pickle
from pyspark.sql import SparkSession
spark = SparkSession.builder.\
        appName("Brazilian_E-Commerce").getOrCreate()

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


dictIntStrStatComm ={ 0:'expediée',
                1:'annulée',
                2:'validée',
                3:'facturée',
                4:'créée',
                5:'livrée',
                6:'indisponible',
                7:'en cours'}

dictStrIntStatComm ={ 'expediée'    :0,
                'annulée'     :1,
                'validée'     :2,
                'facturée'    :3,
                'créée'       :4,
                'livrée'      :5,
                'indisponible':6,
                'en cours'    :7  }

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

#-------------------------------------------------------------------------------------
# vendeurs.columns
#-------------------------------------------------------------------------------------
commandeId = commandes.select('order_id')\
                      .distinct()\
                      .orderBy('order_id')\
                      .toPandas()

dictStrIntCommId ={ cat:i for i,cat
             in enumerate(commandeId.order_id.values)}

vendeurId = vendeurs.select('seller_id')\
                      .distinct()\
                      .orderBy('seller_id')\
                      .toPandas()

dictStrIntVendId ={ cat:i for i,cat
             in enumerate(vendeurId.seller_id.values)}

produitId = produits.select('product_id')\
                      .distinct()\
                      .orderBy('product_id')\
                      .toPandas()

dictStrIntProdId ={ cat:i for i,cat
             in enumerate(produitId.product_id.values)}

clientUId = clients.select('customer_unique_id')\
                      .distinct()\
                      .orderBy('customer_unique_id')\
                      .toPandas()

dictStrIntCliUId ={ cat:i for i,cat
             in enumerate(clientUId.customer_unique_id.values)}

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

pageId = mql.select('landing_page_id')\
                      .distinct()\
                      .orderBy('landing_page_id')\
                      .toPandas()

dictStrIntPageId ={ cat:i for i,cat
             in enumerate(pageId.landing_page_id.values)}

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

typeProspects = ventes.select('lead_type')\
                      .distinct()\
                      .orderBy('lead_type')\
                      .toPandas()

dictStrIntProsp ={ cat:i for i,cat
             in enumerate(typeProspects.lead_type.values)}

typeProspects.fillna('non renseigné', inplace=True)

dictIntStrProsp ={ i:cat for i,cat
             in enumerate(typeProspects.lead_type.values)}


typeComportements = ventes.select('lead_behaviour_profile')\
                      .distinct()\
                      .orderBy('lead_behaviour_profile')\
                      .toPandas()

dictStrIntComp ={ cat:i for i,cat
             in enumerate(typeComportements.lead_behaviour_profile.values)}

typeComportements.fillna('non renseigné', inplace=True)

dictIntStrComp ={ i:cat for i,cat
             in enumerate(typeComportements.lead_behaviour_profile.values)}

#-------------------------------------------------------------------------------------
pickle_file = "dictionnaires.pickle"

with open(pickle_file, 'wb') as f:
    pickle.dump( dictStrIntStatComm ,f)
    pickle.dump( dictIntStrStatComm ,f)
    pickle.dump( dictStrIntCat      ,f)
    pickle.dump( dictIntStrCat      ,f)
    pickle.dump( dictStrIntCommId   ,f)
    pickle.dump( dictStrIntVendId   ,f)
    pickle.dump( dictStrIntProdId   ,f)
    pickle.dump( dictStrIntCliUId   ,f)
    pickle.dump( dictStrIntOrig     ,f)
    pickle.dump( dictIntStrOrig     ,f)
    pickle.dump( dictStrIntPageId   ,f)
    pickle.dump( dictStrIntSegm     ,f)
    pickle.dump( dictIntStrSegm     ,f)
    pickle.dump( dictStrIntProsp    ,f)
    pickle.dump( dictIntStrProsp    ,f)
    pickle.dump( dictStrIntComp     ,f)
    pickle.dump( dictIntStrComp     ,f)
