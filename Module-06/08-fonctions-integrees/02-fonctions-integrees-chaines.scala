import org.apache.spark.sql.functions._

villes.select('ville,
               lpad('ville, 6, "-").alias("lpad"),
               rpad('ville, 6, "-").alias("rpad"),
               lpad(rpad('ville, 18, "-"), 20, "-").alias("pad"),
               trim(lpad(rpad('ville, 18, "-"), 20, "-"),"-").alias("trim"),
               ).show(truncate=false)

villes.select('ville,
              lower('ville).alias("lower"),
              upper('ville).alias("upper"),
              initcap(regexp_replace('ville,"-"," ")).alias("initcap")).
       show(truncate=false)


val donnees = spark.read.
              format("delta").load("donnees/delta/PRODUITS_delta").
              select("QUANTITE").cache()

donnees.select('QUANTITE,
              regexp_replace('QUANTITE,"(\\()(.*)(\\))","").alias("regexp_replace"),
              regexp_extract('QUANTITE,"(\\(.*\\))",0).alias("regexp_extract")).
       select('QUANTITE,'regexp_replace,'regexp_extract,
              concat('regexp_replace,
                   regexp_replace('regexp_extract,"[\\(\\)]","")).alias("concat")
              ).
       where(instr('QUANTITE,"(") > 0).show(5,false)


donnees.where('QUANTITE.rlike("^(10).*(pièces)$")).
       show(false)

donnees.where('QUANTITE.rlike("(sacs).*(pièces)")).
       show(false)

donnees.where('QUANTITE.rlike("^2.*(carton|pièces|bouteilles)")).
       show(false)

donnees.where('QUANTITE.rlike("l{2,}.*0{2}")).
       show(false)

donnees.where('QUANTITE.rlike("^\\d{2,3}\\s\\w{3,6}\\s[\\(]\\d{1,3}\\s\\w?(g[\\)])$")).
       show(false)

donnees.where('QUANTITE.rlike("^\\d")).
       show(false)
