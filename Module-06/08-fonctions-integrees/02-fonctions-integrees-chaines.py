from pyspark.sql.functions import *
spark = SparkSession.builder\
          .config("spark.jars.packages",
                         "io.delta:delta-core_2.12:0.8.0") \
          .config("spark.sql.extensions",
                         "io.delta.sql.DeltaSparkSessionExtension")\
          .getOrCreate()


villes.select('ville',
               lpad('ville', 6, '-').alias('lpad'),
               rpad('ville', 6, '-').alias('rpad'),
               lpad(rpad('ville', 18, '-'), 20, '-').alias('pad'),
               trim(lpad(rpad('ville', 18, ' '), 20, ' ')).alias('trim'),
               )\
      .show(truncate=False)



villes.select('ville',
               lower('ville').alias('lower'),
               upper('ville').alias('upper'),
               initcap(regexp_replace('ville','-',' ')).alias('initcap'),
               reverse('ville').alias('reverse'))\
      .show(truncate=False)

villes.select('ville',
               lower('ville').alias('lower'),
               upper('ville').alias('upper'),
               initcap(regexp_replace('ville','-',' ')).alias('initcap'))\
      .show(truncate=False)


donnees = spark.read\
               .format('delta').load('donnees/delta/PRODUITS_delta')\
               .select('QUANTITE').cache()

donnees.select('QUANTITE',
               regexp_replace('QUANTITE','(\()(.*)(\))','').alias('regexp_replace'),
               regexp_extract('QUANTITE','(\(.*\))',0).alias('regexp_extract'))\
        .select('QUANTITE','regexp_replace','regexp_extract',
               concat('regexp_replace',
                    regexp_replace('regexp_extract','[\(\)]','')).alias('concat')
               )\
        .where(instr('QUANTITE','(') > 0).show(5,truncate=False)

donnees.where(donnees['QUANTITE'].rlike('^(10).*(pièces)$')).show(truncate=False)
donnees.where(donnees['QUANTITE'].rlike('(sacs).*(pièces)')).show(truncate=False)
donnees.where(donnees['QUANTITE'].rlike('^2.*(carton|pièces|bouteilles)')).show(truncate=False)
donnees.where(donnees['QUANTITE'].rlike('l{2,}.*0{2}')).show(truncate=False)
donnees.where(donnees['QUANTITE'].rlike('^\\d{2,3}\\s\\w{3,6}\\s[\\(]\\d{1,3}\\s\\w?(g[\\)])$')).show(truncate=False)
donnees.where(donnees['QUANTITE'].rlike('^\\d')).show(truncate=False)
