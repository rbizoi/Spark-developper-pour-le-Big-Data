donnees = spark.read.format('binaryFile').option('pathGlobFilter', '*.jpg').load('donnees/images')
