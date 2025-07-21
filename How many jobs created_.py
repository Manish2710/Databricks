# Databricks notebook source
# MAGIC %md
# MAGIC **Read CSV File Without any option**

# COMMAND ----------

df_no_option = spark.read.format("csv").load("/FileStore/tables/babies/baby_names_input/baby_names.csv")

# COMMAND ----------

display(df_no_option)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read CSV File With inferSchema option**

# COMMAND ----------

df_infer_schema = spark.read.format("csv").option("inferSchema", True).load("/FileStore/tables/babies/baby_names_input/baby_names.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC **Read CSV File With schema defined option**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("Year", IntegerType(), False),
    StructField("FirstName", StringType(), True),
    StructField("County", StringType(), True),
    StructField("Sex", StringType(), True),
    StructField("Count", IntegerType(), True)
])

# COMMAND ----------

df_defined_schema = spark.read.format("csv").schema(schema).load("/FileStore/tables/babies/baby_names_input/baby_names.csv")

# COMMAND ----------

