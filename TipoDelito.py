%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, when, col, collect_set
from pyspark.sql import Row


spark = SparkSession.builder.master("local").appName("Tipo delito job").enableHiveSupport().getOrCreate()


csv_file_path = "file:///home/mario/Descargas/csv/Crimes_2001_to_Present.csv"

df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df_elegidas = df.select("IUCR", "Primary Type", "Description",)

df_sinDuplicados = df_elegidas.dropDuplicates()

df_formateado = df_sinDuplicados.withColumnRenamed("Primary Type", "TipoPrimario") \
                          .withColumnRenamed("Description", "TipoSecundario") \
                          

df_final = df_formateado.withColumn("idTipoDelito", monotonically_increasing_id())
                          

df_final.show()
df_final.write.mode("overwrite").saveAsTable("mydb.TipoDelito")


    
spark.stop()
