%spark.pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

    
spark = SparkSession.builder.master("local").appName("Horas Job").enableHiveSupport().getOrCreate()

schema = StructType([
    StructField("idHora", IntegerType(), False),
    StructField("descripcion", StringType(), False),
    StructField("hora", IntegerType(), False),
    StructField("minuto", IntegerType(), False)
])

horas = range(24)
minutos = range(60)

data = []
id_hora = 0
for hora in horas:
    for minuto in minutos:
        descripcion = f"{hora:02d}:{minuto:02d}"
        data.append((id_hora, descripcion, hora, minuto))
        id_hora += 1

df = spark.createDataFrame(data, schema)

df.show()

df.write.mode("overwrite").saveAsTable("mydb.hora")

spark.stop()


