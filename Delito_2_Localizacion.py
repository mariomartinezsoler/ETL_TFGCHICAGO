%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, date_format, unix_timestamp, from_unixtime, substring
from pyspark.sql.window import Window



spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Crimen job-Localizacion") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

csv_file_path_inicial = "file:///home/mario/Descargas/csv/tempCrimen/horayfecha.csv"
df_inicial = spark.read.csv(csv_file_path_inicial, header=True, inferSchema=True)

csv_file_path = "file:///home/mario/Descargas/csv/Crimes_2001_to_Present.csv"
df_crimen = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df_localizacion = spark.sql("SELECT * FROM mydb.localizacion")
df_crimen_localizacion = df_crimen.select("Case Number", "Location Description", 
"Block", "Beat", "District", "Ward","X COORDINATE", "Y COORDINATE", "LATITUDE", "LONGITUDE")

df_crimen_localizacion = df_crimen_localizacion.withColumn("bloque", substring(col("Block"), 1, 5))
df_crimen_localizacion = df_crimen_localizacion.withColumn("direccion", substring(col("Block"), 6, 200)) 

# Realizar el join con la tabla de localización
df_unido_localizacion = df_crimen_localizacion.join(df_localizacion, 
    (df_crimen_localizacion["Beat"] == df_localizacion["Beat"]) & 
    (df_crimen_localizacion["District"] == df_localizacion["Distrito"]) & 
    (df_crimen_localizacion["X Coordinate"] == df_localizacion["CoordenadaX"]) & 
    (df_crimen_localizacion["Y Coordinate"] == df_localizacion["CoordenadaY"]) &
    (df_crimen_localizacion["LATITUDE"] == df_localizacion["Latitud"]) &
    (df_crimen_localizacion["LONGITUDE"] == df_localizacion["Longitud"]) &
    (df_crimen_localizacion["bloque"] == df_localizacion["Bloque"]) &
    (df_crimen_localizacion["direccion"] == df_localizacion["Direccion"]) &
    (df_crimen_localizacion["Location Description"] == df_localizacion["TipoLocalizacion"]) &
    (df_crimen_localizacion["Ward"] == df_localizacion["Ward"]), how="left")

df_unido_localizacion.show()

df_final = df_inicial.join(df_unido_localizacion, df_inicial["NumeroCaso"] == df_unido_localizacion["Case Number"], how="left")

df_final = df_final.select("NumeroCaso", "Hora_idHora", "Fecha_idFecha", "idLocalizacion")

df_final = df_final.withColumnRenamed("idLocalizacion", "Localizacion_idLocalizacion")

df_final = df_final.dropDuplicates()

df_final.show()

output_path = "file:///home/mario/Descargas/csv/tempCrimen2"
df_final.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

import os
import shutil

if not os.path.exists(os.path.dirname("/home/mario/Descargas/csv/tempCrimen2/localizacion.csv")):
    os.makedirs(os.path.dirname("/home/mario/Descargas/csv/tempCrimen2/localizacion.csv"))

csv_directory = "/home/mario/Descargas/csv/tempCrimen2"
files = os.listdir(csv_directory)

for file_name in files:
    if file_name.startswith("part-") and file_name.endswith(".csv"):
        shutil.move(os.path.join(csv_directory, file_name), os.path.join(csv_directory, "localizacion.csv"))




