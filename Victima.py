%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local").appName("Victima job").getOrCreate()

csv_file_path_victima = "file:///home/mario/Descargas/csv/Violence_Reduction_Victims_of_Homicides_and_NonFatal_Shootings.csv"

df_victima = spark.read.csv(csv_file_path_victima, header=True, inferSchema=True)

df_victima = df_victima.select("AGE", "SEX", "RACE", "GUNSHOT_INJURY_I")

df_formateado = df_victima.withColumnRenamed("AGE", "RangoEdad") \
                        .withColumnRenamed("SEX", "Sexo") \
                        .withColumnRenamed("RACE", "Raza") \
                        .withColumnRenamed("GUNSHOT_INJURY_I", "Disparo?")

df_formateado = df_formateado.withColumn("Victima?", lit(True))

df_formateado = df_formateado.dropDuplicates()
                        
window = Window.orderBy("RangoEdad")

df_final = df_formateado.withColumn("idVictima", row_number().over(window) - 1)

df_final.show()

df_final.write.mode("overwrite").saveAsTable("mydb.Victima")
