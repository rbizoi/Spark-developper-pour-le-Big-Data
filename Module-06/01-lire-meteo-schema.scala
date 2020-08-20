import org.apache.spark.sql.types._

val customSchemaMeteo = StructType(
    StructField("station"                                                  , StringType , true)::
    StructField("date_utc"                                                 , StringType , true)::
    StructField("pression_au_niveau_mer"                                   , IntegerType, true)::
    StructField("variation_de_pression_en_3_heures"                        , IntegerType, true)::
    StructField("type_de_tendance_barometrique"                            , IntegerType, true)::
    StructField("direction_du_vent_moyen_10_mn"                            , IntegerType, true)::
    StructField("vitesse_du_vent_moyen_10_mn"                              , FloatType  , true)::
    StructField("temperature"                                              , FloatType  , true)::
    StructField("point_de_rosee"                                           , FloatType  , true)::
    StructField("humidite"                                                 , IntegerType, true)::
    StructField("visibilite_horizontale"                                   , FloatType  , true)::
    StructField("temps_present"                                            , IntegerType, true)::
    StructField("temps_passe_1"                                            , IntegerType, true)::
    StructField("temps_passe_2"                                            , IntegerType, true)::
    StructField("nebulosite_totale"                                        , FloatType  , true)::
    StructField("nebulosite_des_nuages_de_l_etage_inferieur"               , IntegerType, true)::
    StructField("hauteur_de_la_base_des_nuages_de_l_etage_inferieur"       , IntegerType, true)::
    StructField("type_des_nuages_de_l_etage_inferieur"                     , IntegerType, true)::
    StructField("type_des_nuages_de_l_etage_moyen"                         , IntegerType, true)::
    StructField("type_des_nuages_de_l_etage_superieur"                     , IntegerType, true)::
    StructField("pression_station"                                         , IntegerType, true)::
    StructField("niveau_barometrique"                                      , IntegerType, true)::
    StructField("geopotentiel"                                             , IntegerType, true)::
    StructField("variation_de_pression_en_24_heures"                       , IntegerType, true)::
    StructField("temperature_minimale_sur_12_heures"                       , FloatType  , true)::
    StructField("temperature_minimale_sur_24_heures"                       , FloatType  , true)::
    StructField("temperature_maximale_sur_12_heures"                       , FloatType  , true)::
    StructField("temperature_maximale_sur_24_heures"                       , FloatType  , true)::
    StructField("temperature_minimale_du_sol_sur_12_heures"                , FloatType  , true)::
    StructField("methode_mesure_tw"                                        , IntegerType, true)::
    StructField("temperature_du_thermometre_mouille"                       , FloatType  , true)::
    StructField("rafales_sur_les_10_dernieres_minutes"                     , FloatType  , true)::
    StructField("rafales_sur_une_periode"                                  , FloatType  , true)::
    StructField("periode_de_mesure_de_la_rafale"                           , FloatType  , true)::
    StructField("etat_du_sol"                                              , IntegerType, true)::
    StructField("hauteur_totale_de_la_couche_de_neige_glace_autre_au_sol"  , FloatType  , true)::
    StructField("hauteur_de_la_neige_fraiche"                              , FloatType  , true)::
    StructField("periode_de_mesure_de_la_neige_fraiche"                    , FloatType  , true)::
    StructField("precipitations_dans_les_1_dernieres_heures"               , FloatType  , true)::
    StructField("precipitations_dans_les_3_dernieres_heures"               , FloatType  , true)::
    StructField("precipitations_dans_les_6_dernieres_heures"               , FloatType  , true)::
    StructField("precipitations_dans_les_12_dernieres_heures"              , FloatType  , true)::
    StructField("precipitations_dans_les_14_dernieres_heures"              , FloatType  , true)::
    StructField("phenomene_special1"                                       , FloatType  , true)::
    StructField("phenomene_special2"                                       , FloatType  , true)::
    StructField("phenomene_special3"                                       , FloatType  , true)::
    StructField("phenomene_special4"                                       , FloatType  , true)::
    StructField("nebulosite_cche_nuageuse_1"                               , IntegerType, true)::
    StructField("type_nuage_1"                                             , IntegerType, true)::
    StructField("hauteur_de_base_1"                                        , IntegerType, true)::
    StructField("nebulosite_cche_nuageuse_2"                               , IntegerType, true)::
    StructField("type_nuage_2"                                             , IntegerType, true)::
    StructField("hauteur_de_base_2"                                        , IntegerType, true)::
    StructField("nebulosite_cche_nuageuse_3"                               , IntegerType, true)::
    StructField("type_nuage_3"                                             , IntegerType, true)::
    StructField("hauteur_de_base_3"                                        , IntegerType, true)::
    StructField("nebulosite_cche_nuageuse_4"                               , IntegerType, true)::
    StructField("type_nuage_4"                                             , IntegerType, true)::
    StructField("hauteur_de_base_4"                                        , IntegerType, true)::Nil )

val meteoDataFrame  = spark.read.format("csv").
    option("sep",";").
    option("header","true").
    option("nullValue","mq").
    option("mergeSchema", "true").
    schema(customSchemaMeteo).
    load("donnees/meteo")
