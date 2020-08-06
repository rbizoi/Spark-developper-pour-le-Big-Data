from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature    import PCA, StandardScaler, VectorAssembler
from pyspark.ml.linalg     import array


donnees = spark.sql("select * from cours_spark.meteoMensuelle order by 1")

modelA = VectorAssembler().\
                setInputCols(
                       ['Janvier', 'Fevrier', 'Mars',
                        'Avril', 'Mai', 'Juin', 'Juillet',
                        'Aout', 'Septembre', 'Octobre',
                        'Novembre', 'Decembre']).\
                 setOutputCol('variables')

modelN = StandardScaler().\
                setInputCol("variables").\
                setOutputCol("vNormalisees").\
                setWithStd(True).\
                setWithMean(False)

modelACP = PCA().\
            setInputCol("vNormalisees").\
            setOutputCol("vACP").\
            setK(2)

modelKM = KMeans().setK(7).\
                   setFeaturesCol("vACP").\
                   setPredictionCol("vKM")

modelPipe = Pipeline(stages=[modelA, modelN, modelACP, modelKM]).fit(donnees)

donneesKM.transform(donnees)
