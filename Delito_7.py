%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, date_format, unix_timestamp, from_unixtime, substring, concat_ws, expr,when, countDistinct, length, to_timestamp
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Crimen job-7") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .enableHiveSupport() \
    .getOrCreate()


csv_file_path_inicial = "file:///home/mario/Descargas/csv/tempCrimen6/victima.csv"
df_inicial = spark.read.csv(csv_file_path_inicial, header=True, inferSchema=True)

csv_file_path_arresto = "file:///home/mario/Descargas/csv/Arrests.csv"
df_arresto = spark.read.csv(csv_file_path_arresto, header=True, inferSchema=True)
df_arresto = df_arresto.select("CASE NUMBER","ARREST DATE")

df_union = df_inicial.join(df_arresto, df_inicial["NumeroCaso"] == df_arresto["CASE NUMBER"], how="left")

df_hora = spark.sql("SELECT * FROM mydb.Hora")
df_fecha = spark.sql("SELECT * FROM mydb.Fecha")

df_unionHora = df_union.join(df_hora, df_union["Hora_idHora"] == df_hora["idHora"], how="left")
df_unionFecha = df_unionHora.join(df_fecha, df_unionHora["Fecha_idFecha"] == df_fecha["idFecha"], how="left")

df_unionFecha = df_unionFecha.withColumn("mes", col("mes").cast("string")).withColumn("dia", col("dia").cast("string"))
df_unionFecha = df_unionFecha.withColumn("mes", when(length(col("mes")) == 1, concat_ws("", lit("0"), col("mes"))).otherwise(col("mes")))
df_unionFecha = df_unionFecha.withColumn("dia", when(length(col("dia")) == 1, concat_ws("", lit("0"), col("dia"))).otherwise(col("dia")))


df_arreglado = df_unionFecha.withColumn(
    "CrimeDate",
    unix_timestamp(
        concat_ws(" ",
                  concat_ws("-", col("anyo"), col("mes"), col("dia")),
                  concat_ws(":", col("hora"), col("minuto"))
        ), "yyyy-MM-dd HH:mm"
    ).cast("timestamp")
)



df_arreglado = df_arreglado.withColumn("ARREST DATE", from_unixtime(unix_timestamp(col("ARREST DATE"), "MM/dd/yyyy hh:mm:ss a")))

df_arreglado = df_arreglado.drop("CASE NUMBER","idHora", "descripcion", "descripcion", "idFecha", "epoca")

df_arreglado = df_arreglado.withColumn("CrimeDate", to_timestamp("CrimeDate"))


df_arreglado = df_arreglado.withColumn(
    "TiempoArresto",
    when(
        col("ARREST DATE").isNotNull() & col("CrimeDate").isNotNull(),
        (unix_timestamp(col("ARREST DATE")) - unix_timestamp(col("CrimeDate"))) / 60
    ).otherwise(None)
)

df_arreglado.filter(col("NumeroCaso") == "JG268448").show()


df_arreglado = df_arreglado.select("NumeroCaso", "Hora_idHora", "Fecha_idFecha", "Localizacion_idLocalizacion", "Arresto_idArresto", "EstacionPolicia_idEstacionPolicia", "TipoDelito_idTipoDelito", "Victima_idVictima", "TiempoArresto")

df_arreglado = df_arreglado.fillna(-1)

df_arreglado.show()

output_path = "file:///home/mario/Descargas/csv/tempCrimen7"
df_arreglado.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

import os
import shutil

if not os.path.exists(os.path.dirname("/home/mario/Descargas/csv/tempCrimen7/medidas1.csv")):
    os.makedirs(os.path.dirname("/home/mario/Descargas/csv/tempCrimen7/medidas1.csv"))

csv_directory = "/home/mario/Descargas/csv/tempCrimen7"
files = os.listdir(csv_directory)

for file_name in files:
    if file_name.startswith("part-") and file_name.endswith(".csv"):
        shutil.move(os.path.join(csv_directory, file_name), os.path.join(csv_directory, "medidas1.csv"))

spark.stop()
