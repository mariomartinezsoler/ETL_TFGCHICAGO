%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, date_format, unix_timestamp, from_unixtime, substring
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Crimen job-Victima") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

csv_file_path_inicial = "file:///home/mario/Descargas/csv/tempCrimen5/tipoDelito.csv"
df_inicial = spark.read.csv(csv_file_path_inicial, header=True, inferSchema=True)

df_tablaVictima = spark.sql("SELECT * FROM mydb.victima")

csv_file_path_victima = "file:///home/mario/Descargas/csv/Violence_Reduction_Victims_of_Homicides_and_NonFatal_Shootings.csv"
df_victima = spark.read.csv(csv_file_path_victima, header=True, inferSchema=True)

df_union_victima = df_inicial.join(df_victima, df_inicial["numeroCaso"] == df_victima["CASE_NUMBER"], how="left")

df_union_formateado = df_union_victima.select("NumeroCaso", "Hora_idHora", "Fecha_idFecha", "Localizacion_idLocalizacion",
"Arresto_idArresto", "EstacionPolicia_idEstacionPolicia", "TipoDelito_idTipoDelito", "Age", "Sex", "Race", "GUNSHOT_INJURY_I")

df_union_formateado.show()


df_unionFinal = df_union_formateado.join(df_tablaVictima, 
    (df_union_formateado["Age"] == df_tablaVictima["RangoEdad"]) & 
    (df_union_formateado["Sex"] == df_tablaVictima["Sexo"]) & 
    (df_union_formateado["Race"] == df_tablaVictima["raza"]) & 
    (df_union_formateado["GUNSHOT_INJURY_I"] == df_tablaVictima["Disparo?"]), how="left")
    
    
df_unionFinal = df_unionFinal.select("NumeroCaso", "Hora_idHora", "Fecha_idFecha", "Localizacion_idLocalizacion", 
"Arresto_idArresto", "EstacionPolicia_idEstacionPolicia", "TipoDelito_idTipoDelito", "idVictima")

df_final = df_unionFinal.withColumnRenamed("idVictima", "Victima_idVictima")

df_final.show()


output_path = "file:///home/mario/Descargas/csv/tempCrimen6"
df_final.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

import os
import shutil

if not os.path.exists(os.path.dirname("/home/mario/Descargas/csv/tempCrimen6/victima.csv")):
    os.makedirs(os.path.dirname("/home/mario/Descargas/csv/tempCrimen6/victima.csv"))

csv_directory = "/home/mario/Descargas/csv/tempCrimen6"
files = os.listdir(csv_directory)

for file_name in files:
    if file_name.startswith("part-") and file_name.endswith(".csv"):
        shutil.move(os.path.join(csv_directory, file_name), os.path.join(csv_directory, "victima.csv"))


