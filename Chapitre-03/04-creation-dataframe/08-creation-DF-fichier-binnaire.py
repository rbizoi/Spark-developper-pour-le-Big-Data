donnees = spark.read.format('binaryFile').option('pathGlobFilter', '*.jpg').load('donnees/images')
donnees.printSchema()
donnees.select('length','path','modificationTime').show(3)

from pyspark.ml import image
donnees = spark.read.format('image').load('donnees/images')
donnees.printSchema()
donnees.select('image.height','image.width',
               'image.nChannels','image.mode').show(3)
