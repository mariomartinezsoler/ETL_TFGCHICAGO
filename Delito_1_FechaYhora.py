%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, date_format, unix_timestamp, from_unixtime
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Crimen job-horayfecha") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

csv_file_path = "file:///home/mario/Descargas/csv/Crimes_2001_to_Present.csv"
df_crimen = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df_hora = spark.sql("SELECT * FROM mydb.Hora")
df_fecha = spark.sql("SELECT * FROM mydb.Fecha")

df_date = df_crimen.select("Date", "case Number")

df_date = df_date.withColumn("timestamp", from_unixtime(unix_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))) \
    .withColumn("hora", date_format(col("timestamp"), "H").cast("int")) \
    .withColumn("minutos", date_format(col("timestamp"), "m").cast("int")) \
    .withColumn("dia", date_format(col("timestamp"), "d").cast("int")) \
    .withColumn("mes", date_format(col("timestamp"), "M").cast("int")) \
    .withColumn("anyo", date_format(col("timestamp"), "yyyy").cast("int"))

df_unido_hora = df_date.join(df_hora, (df_date["hora"] == df_hora["hora"]) & (df_date["minutos"] == df_hora["minuto"]), how="left")

df_unido_fecha = df_unido_hora.join(df_fecha, (df_unido_hora["dia"] == df_fecha["dia"]) & (df_unido_hora["mes"] == df_fecha["mes"]) 
& (df_unido_hora["anyo"] == df_fecha["anyo"]), how="left")

df_final = df_unido_fecha.select("case Number", "idHora", "idFecha")


df_final = df_final.withColumnRenamed("case Number", "NumeroCaso") \
    .withColumnRenamed("idHora", "Hora_idHora") \
    .withColumnRenamed("idFecha", "Fecha_idFecha")

df_final.show()

df_final = df_final.dropDuplicates()

output_path = "file:///home/mario/Descargas/csv/tempCrimen"
df_final.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

import os
import shutil

if not os.path.exists(os.path.dirname("/home/mario/Descargas/csv/tempCrimen/horayfecha.csv")):
    os.makedirs(os.path.dirname("/home/mario/Descargas/csv/tempCrimen/horayfecha.csv"))

csv_directory = "/home/mario/Descargas/csv/tempCrimen"
files = os.listdir(csv_directory)

for file_name in files:
    if file_name.startswith("part-") and file_name.endswith(".csv"):
        shutil.move(os.path.join(csv_directory, file_name), os.path.join(csv_directory, "horayfecha.csv"))


