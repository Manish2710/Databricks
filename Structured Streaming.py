# Databricks notebook source
# MAGIC %md
# MAGIC **Schema for Structured Streaming**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_defined = StructType([StructField('File', StringType(), True),
                             StructField('Shop', StringType(), True),
                             StructField('Sale_count', IntegerType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC **Create a Folder Structure in DBFS file system**

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/stream_checkpoint/")
dbutils.fs.mkdirs("/FileStore/tables/stream_read/")
dbutils.fs.mkdirs("/FileStore/tables/stream_write/")



# COMMAND ----------

# MAGIC %md
# MAGIC **Read streaming data**

# COMMAND ----------

df = spark.readStream.format('csv').schema(schema_defined).option("header", True).option("sep",";").load("/FileStore/tables/stream_read/")

df1 = df.groupBy("Shop").sum("Sale_count")
display(df1)

# COMMAND ----------

df4 = df.writeStream.format("parquet").outputMode("append").option("path", "/FileStore/tables/stream_write/").option("checkpointLocation")