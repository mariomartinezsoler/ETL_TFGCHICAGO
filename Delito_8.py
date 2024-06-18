%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, date_format, unix_timestamp, from_unixtime, substring, concat_ws, expr,when, countDistinct
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Crimen job-8") \
    .enableHiveSupport() \
    .getOrCreate()
    
csv_file_path_inicial = "file:///home/mario/Descargas/csv/tempCrimen7/medidas1.csv"
df_inicial = spark.read.csv(csv_file_path_inicial, header=True, inferSchema=True)

df_NumArrestos = df_inicial.groupBy("NumeroCaso").agg(countDistinct("Arresto_idArresto").alias("NumeroArrestos"))

df_NuevoNumArrestos = df_inicial.join(df_NumArrestos, on="NumeroCaso", how="left")

df_NuevoNumArrestos = df_NuevoNumArrestos.withColumn("NumeroArrestos", 
when(col("Arresto_idArresto") == -1, 0).otherwise(col("NumeroArrestos")))

df_NuevoNumArrestos.show()

output_path = "file:///home/mario/Descargas/csv/tempCrimen8"
df_NuevoNumArrestos.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

import os
import shutil

if not os.path.exists(os.path.dirname("/home/mario/Descargas/csv/tempCrimen8/medidas1.csv")):
    os.makedirs(os.path.dirname("/home/mario/Descargas/csv/tempCrimen8/medidas2.csv"))

csv_directory = "/home/mario/Descargas/csv/tempCrimen8"
files = os.listdir(csv_directory)

for file_name in files:
    if file_name.startswith("part-") and file_name.endswith(".csv"):
        shutil.move(os.path.join(csv_directory, file_name), os.path.join(csv_directory, "medidas2.csv"))

spark.stop()



