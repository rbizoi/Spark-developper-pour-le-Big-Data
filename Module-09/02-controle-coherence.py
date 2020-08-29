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


['order_id', 'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date', 'price', 'freight_value']

details_commandes.select("order_id").distinct().count()))



print('details_commandes   = %d\n\
order_id            = %d\n\
shipping_limit_date = %d'%(
      details_commandes.count(),
      details_commandes.select('order_id').distinct().count(),
      details_commandes.groupBy('order_id')\
                 .agg(countDistinct('shipping_limit_date').alias('nb'))\
                 .where('nb == 2').count()))

details_commandes.groupBy('order_id')\
           .agg(countDistinct('shipping_limit_date').alias('nb'))\
           .where('nb == 2').count()

liste = details_commandes.groupBy('order_id')\
           .agg(countDistinct('shipping_limit_date').alias('nb'))\
           .where('nb == 2').select('order_id')

commandes.join(liste,'order_id').orderBy('order_id').show(10)
details_commandes.join(liste,'order_id').orderBy('order_id','order_item_id').show(10)




details_commandes.groupBy('seller_id')\
                 .agg(
                        countDistinct('order_id').alias('nb_commandes'),
                         countDistinct('product_id').alias('nb_produits')).orderBy(desc('nb_commandes')).show()
