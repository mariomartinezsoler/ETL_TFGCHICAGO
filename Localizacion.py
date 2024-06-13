%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, when, col, substring, row_number
from pyspark.sql import Row, Window


spark = SparkSession.builder.master("local").appName("Localizacion job").enableHiveSupport().getOrCreate()


csv_file_path = "file:///home/mario/Descargas/csv/Crimes_2001_to_Present.csv"

df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df_elegidas = df.select("Location Description", "Block", "Beat", 
"District", "Ward","X COORDINATE", "Y COORDINATE", "LATITUDE", "LONGITUDE")


df_elegidas = df_elegidas.withColumn("bloque", substring(col("Block"), 1, 5))
df_elegidas = df_elegidas.withColumn("direccion", substring(col("Block"), 6, 200)) 

df_elegidas = df_elegidas.drop("Block")

df_formateado = df_elegidas.withColumnRenamed("Location Description", "TipoLocalizacion") \
                          .withColumnRenamed("District", "Distrito") \
                          .withColumnRenamed("X COORDINATE", "COORDENADAX") \
                          .withColumnRenamed("Y COORDINATE", "COORDENADAY") \
                          .withColumnRenamed("LATITUDE", "LATITUD") \
                          .withColumnRenamed("LONGITUDE", "LONGITUD")  

df_sinDuplicados = df_formateado.dropDuplicates()

window = Window.orderBy("Distrito")  

df_final = df_sinDuplicados.withColumn("idLocalizacion", row_number().over(window) -1)

df_final.show()
df_final.write.mode("overwrite").saveAsTable("mydb.Localizacion")


    
spark.stop()
