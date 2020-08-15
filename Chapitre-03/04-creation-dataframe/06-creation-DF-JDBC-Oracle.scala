//spark-shell \
//    --master spark://jupiter.olimp.fr:7077 \
//    --executor-cores 8 \
//    --executor-memory 20g \
//    --jars /usr/share/spark/jars/ojdbc8.jar

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.
              config("spark.jars.packages",
                             "oracle:ojdbc8").
              config("spark.jars.packages",
                             "io.delta:delta-core_2.12:0.8.0").
              config("spark.sql.extensions",
                             "io.delta.sql.DeltaSparkSessionExtension").
              getOrCreate()

val url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.1.25)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=pdbcours1)))"
val user = "stagiaire"
val password = "CoursSPARK3#"
val format     = "delta"
val repertoire = "donnees/delta/"

val donnees = spark.read.format("delta").load(
                "donnees/delta/FOURNISSEURS_delta")

donnees.write.format("jdbc").
        option("url", url).
        option("dbtable", "FOURNISSEURS").
        option("user", user).
        option("password", password).
        option("driver", "oracle.jdbc.OracleDriver").
        save()

spark.read.
        format("jdbc").
        option("url", url).
        option("dbtable", "FOURNISSEURS").
        option("user", user).
        option("password", password).
        option("driver", "oracle.jdbc.OracleDriver").
        load().
        select("SOCIETE","TELEPHONE").
        show(3)
