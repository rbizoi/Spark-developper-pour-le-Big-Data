from pyspark.sql.types import StructType, \
     StructField, FloatType, \
     IntegerType, StringType

def transformLigneMeteo(ligne):
   champs = ligne.split(';')
   return ( str(champs[0]),
            (int(str(champs[1])[0:4]),
            int(str(champs[1])[4:6]),
            int(str(champs[1])[6:8]),
            float(str(champs[7]))      - 273.15,
            float(int(str(champs[9]))  / 100 ),
            float(int(str(champs[10])) / 1000 ),
            float(int(str(champs[20])) / 1000 )) )

def transformLignePoste(ligne):
    champs = ligne.split(';')
    return ( str(champs[0]),
            (str(champs[1]).title(),
            float(champs[2]),
            float(champs[3]),
            int(champs[4])))

donnees00 = spark.sparkContext. \
       textFile('/user/spark/donnees/meteo.txt').\
       filter( lambda ligne :
                 str(ligne)[0:5].isdigit()).\
        map(lambda ligne:
                 str(ligne).replace('mq','0')).\
        map(lambda ligne:
                  transformLigneMeteo(ligne)).\
        persist()

donnees01 = spark.sparkContext.\
       textFile('/user/spark/donnees/postesSynop.csv').\
       filter( lambda ligne :
                 str(ligne)[0].isdigit() ).\
       map( lambda ligne :
                 transformLignePoste(ligne)).\
       join(donnees00).\
       map(lambda ligne : tuple([ligne[0]] +
                 [x for x in ligne[1][0]] +
                 [x for x in ligne[1][1]]) ).persist()

donneesMeteo = donnees01.toDF(['Id','ville','latitude','longitude',
                  'altitude','annee','mois','jour','temperature',
                  'humidite','visibilite','pression']).cache()
donneesMeteo.printSchema()

schema = StructType([
        StructField('Id'           , StringType() , True),
        StructField('ville'        , StringType() , True),
        StructField('latitude'     , FloatType() , True),
        StructField('longitude'    , FloatType() , True),
        StructField('altitude'     , IntegerType() , True),
        StructField('annee'        , IntegerType(), True),
        StructField('mois'         , IntegerType(), True),
        StructField('jour'         , IntegerType(), True),
        StructField('temperature'  , FloatType()  , True),
        StructField('humidite'     , FloatType()  , True),
        StructField('visibilite'   , FloatType()  , True),
        StructField('pression'     , FloatType()  , True)])

donneesMeteo01 = spark.createDataFrame(donnees01, schema).cache()
donneesMeteo01.printSchema()
donneesMeteo01.show(3)
donneesMeteo01.select('ville','annee','mois','jour','temperature').show(3)
