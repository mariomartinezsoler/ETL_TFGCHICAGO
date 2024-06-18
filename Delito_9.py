%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, date_format, unix_timestamp, from_unixtime, substring, concat_ws, expr,when, countDistinct
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Crimen job-9") \
    .config("spark.sql.shuffle.partitions", "1000") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()



csv_file_path_inicial = "file:///home/mario/Descargas/csv/tempCrimen8/medidas2.csv"
df_inicial = spark.read.csv(csv_file_path_inicial, header=True, inferSchema=True)

df_numVictimas = df_inicial.groupBy("NumeroCaso").agg(countDistinct("Victima_idVictima").alias("NumeroVictimas"))

df_final = df_inicial.join(df_numVictimas, on="NumeroCaso", how="left")

df_final = df_final.withColumn("NumeroVictimas", when(col("Victima_idVictima") == -1, 0).otherwise(col("NumeroVictimas")))

df = df_final.dropDuplicates()

window = Window.orderBy("NumeroCaso")  

df = df.withColumn("idDelito", row_number().over(window) -1)

df.show()

df.write.mode("overwrite").saveAsTable("mydb.delito")

