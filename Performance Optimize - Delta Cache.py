# Databricks notebook source
dbutils.fs.mkdirs("/FileStore/tables/bigdata_training/read_files")

# COMMAND ----------

from pyspark.sql.types import *

csvSchema = StructType([
    StructField("Year", IntegerType()),
    StructField("First_Name", StringType()),
    StructField("County", StringType()),
    StructField("Gender", StringType()),
    StructField("Count", IntegerType())
])

df = spark.read.option("sep", ",").option("header", True).schema(csvSchema).csv("/FileStore/tables/bigdata_training/read_files/baby_names.csv")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Delta table**

# COMMAND ----------

df.write.format("delta").saveAsTable("names")

# COMMAND ----------

# MAGIC %md
# MAGIC **Enable Delta Cache**

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC **Select comes from DBFS**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from names

# COMMAND ----------

# MAGIC %md
# MAGIC **Preload and Cache Entire Delta Table into Local Disk**

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE SELECT * FROM names

# COMMAND ----------

# MAGIC %md
# MAGIC **This time Select comes from Local Disk**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM names WHERE Year = 2007

# COMMAND ----------

