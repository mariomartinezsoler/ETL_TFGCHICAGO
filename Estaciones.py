%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, when, col
from pyspark.sql import Row


spark = SparkSession.builder.master("local").appName("Police Stations job").enableHiveSupport().getOrCreate()


csv_file_path = "file:///home/mario/Descargas/csv/PoliceStations.csv"

df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df_elegidas = df.select("DISTRICT", "ZIP", "X COORDINATE", "Y COORDINATE", "LATITUDE", "LONGITUDE", "DISTRICT NAME")

df_formateado = df_elegidas.withColumn("idEstacionPolicia", monotonically_increasing_id())

df_formateado = df_formateado.withColumnRenamed("DISTRICT", "DISTRITO") \
                          .withColumnRenamed("X COORDINATE", "COORDENADAX") \
                          .withColumnRenamed("Y COORDINATE", "COORDENADAY") \
                          .withColumnRenamed("LATITUDE", "LATITUD") \
                          .withColumnRenamed("LONGITUDE", "LONGITUD") \
                          .withColumnRenamed("DISTRICT NAME", "NOMBRE")

df_final = df_formateado.withColumn("DISTRITO", when(col("DISTRITO") == "Headquarters", 0).otherwise(col("DISTRITO")))

df_final.write.mode("overwrite").saveAsTable("mydb.estacionpolicia")
df_final.show()

spark.stop()




