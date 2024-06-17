%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, date_format, unix_timestamp, from_unixtime, substring
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Crimen job-TipoDelito") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

csv_file_path_inicial = "file:///home/mario/Descargas/csv/tempCrimen4/estaciones.csv"
df_inicial = spark.read.csv(csv_file_path_inicial, header=True, inferSchema=True)

df_tipoDelito = spark.sql("SELECT * FROM mydb.tipoDelito")

csv_file_path = "file:///home/mario/Descargas/csv/Crimes_2001_to_Present.csv"
df_crimen = spark.read.csv(csv_file_path, header=True, inferSchema=True)



df_union_crimen = df_inicial.join(df_crimen, df_inicial["numeroCaso"] == df_crimen["CASE NUMBER"], how="left")

df_union_formateado = df_union_crimen.select("NumeroCaso", "Hora_idHora", "Fecha_idFecha", "Localizacion_idLocalizacion", "Arresto_idArresto", "EstacionPolicia_idEstacionPolicia", "IUCR", "Primary Type", "Description")

df_union_formateado.show()

df_unionFinal = df_union_formateado.join(df_tipoDelito, 
    (df_union_formateado["IUCR"] == df_tipoDelito["IUCR"]) & 
    (df_union_formateado["Primary Type"] == df_tipoDelito["TipoPrimario"]) & 
    (df_union_formateado["Description"] == df_tipoDelito["TipoSecundario"]), how="left")
    
df_unionFinal = df_unionFinal.select("NumeroCaso", "Hora_idHora", "Fecha_idFecha", "Localizacion_idLocalizacion", "Arresto_idArresto", "EstacionPolicia_idEstacionPolicia", "idTipoDelito")

df_final = df_unionFinal.withColumnRenamed("idTipoDelito", "TipoDelito_idTipoDelito")

output_path = "file:///home/mario/Descargas/csv/tempCrimen5"
df_final.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

import os
import shutil

if not os.path.exists(os.path.dirname("/home/mario/Descargas/csv/tempCrimen5/tipoDelito.csv")):
    os.makedirs(os.path.dirname("/home/mario/Descargas/csv/tempCrimen5/tipoDelito.csv"))

csv_directory = "/home/mario/Descargas/csv/tempCrimen5"
files = os.listdir(csv_directory)

for file_name in files:
    if file_name.startswith("part-") and file_name.endswith(".csv"):
        shutil.move(os.path.join(csv_directory, file_name), os.path.join(csv_directory, "tipoDelito.csv"))

spark.stop()



