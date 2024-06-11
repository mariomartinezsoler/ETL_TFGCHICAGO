%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, sequence, year, month, dayofmonth, when, date_format, lit, expr, monotonically_increasing_id
from pyspark.sql.types import DateType
from datetime import datetime

spark = SparkSession.builder.master("local").appName("Fecha job").enableHiveSupport().getOrCreate()

df = spark.createDataFrame([(1,)], ["idFecha"])

df = df.withColumn("fecha", explode(expr("sequence(to_timestamp('1991-01-01'), to_timestamp('2024-12-31'), interval 1 day)")))

df = df.withColumn("idFecha", monotonically_increasing_id())

df = df.withColumn("anyo", year("fecha").cast("int")) \
    .withColumn("mes", month("fecha").cast("int")) \
    .withColumn("dia", dayofmonth("fecha").cast("int")) \
    .withColumn("epoca", 
    when(
        ((month("fecha") == 3) & (dayofmonth("fecha") >= 20)) |
        ((month("fecha") >= 4) & (month("fecha") <= 5)) |
        ((month("fecha") == 6) & (dayofmonth("fecha") < 21)), 
        "primavera"
    )
    .when(
        ((month("fecha") == 6) & (dayofmonth("fecha") >= 21)) |
        ((month("fecha") >= 7) & (month("fecha") <= 8)) |
        ((month("fecha") == 9) & (dayofmonth("fecha") < 22)), 
        "verano"
    )
    .when(
        ((month("fecha") == 9) & (dayofmonth("fecha") >= 22)) |
        ((month("fecha") >= 10) & (month("fecha") <= 11)) |
        ((month("fecha") == 12) & (dayofmonth("fecha") < 21)), 
        "otoño"
    )
    .otherwise("invierno")
    ) \
    .withColumn("descripcion", date_format("fecha", "dd/MM/yyyy")) \
    .select("idFecha", "descripcion", "anyo", "mes", "epoca", "dia")

df.show()

df.write.mode("overwrite").saveAsTable("mydb.fecha")

spark.stop()


