%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, when, col, substring, row_number
from pyspark.sql import Row, Window


spark = SparkSession.builder.master("local").appName("Arresto job").enableHiveSupport().getOrCreate()


csv_file_path_crimen = "file:///home/mario/Descargas/csv/Crimes_2001_to_Present.csv"
csv_file_path_arresto = "file:///home/mario/Descargas/csv/Arrests.csv"

df_crimen = spark.read.csv(csv_file_path_crimen, header=True, inferSchema=True)
df_arresto = spark.read.csv(csv_file_path_arresto, header=True, inferSchema=True)

df_arresto = df_arresto.select("CASE NUMBER","Race", "CHARGE 1 DESCRIPTION", "CHARGE 1 TYPE", "CHARGE 1 CLASS")

df_crimen = df_crimen.select("Case Number", "Arrest")

df_crimen = df_crimen.withColumnRenamed("Case Number", "Numero de caso")

df_union = df_crimen.join(df_arresto, df_crimen["Numero de caso"] == df_arresto["CASE NUMBER"], "left")

df_union.show();

df_formateado = df_union.withColumnRenamed("Race", "Raza") \
                          .withColumnRenamed("CHARGE 1 DESCRIPTION", "CargoMasGrave") \
                          .withColumnRenamed("CHARGE 1 TYPE", "TipoCargo") \
                          .withColumnRenamed("CHARGE 1 CLASS", "ClaseCargo") \
                          .withColumnRenamed("Arrest", "Arresto?")
                          
df_sinDuplicados = df_formateado.dropDuplicates(["Raza", "CargoMasGrave", "TipoCargo", "ClaseCargo", "Arresto?"])                          

window = Window.orderBy("Numero de caso")  

df_final = df_sinDuplicados.withColumn("idArresto", row_number().over(window) -1)

df_final = df_final.drop("Numero de caso")
df_final = df_final.drop("CASE NUMBER")

df_final.show()
df_final.write.mode("overwrite").saveAsTable("mydb.Arresto")
