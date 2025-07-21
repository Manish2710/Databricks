# Databricks notebook source
df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/FileStore/tables/corrupt_data.csv")

display(df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([StructField("Month", StringType(), True), StructField("Emp_count", IntegerType(), True), StructField("Production_unit", IntegerType(), True), StructField("Expense", IntegerType(), True), StructField("_corrupt_record", StringType(), True)])

# COMMAND ----------

df = spark.read.format("csv").option("mode", "PERMISSIVE").option("header", True).schema(schema).load("/FileStore/tables/corrupt_data.csv")

display(df)

# COMMAND ----------

df1 = spark.read.format("csv").option("mode", "DROPMALFORMED").option("header", True).schema(schema).load("/FileStore/tables/corrupt_data.csv")

display(df1)

# COMMAND ----------

df1 = spark.read.format("csv").option("mode", "FAILFAST").option("header", True).schema(schema).load("/FileStore/tables/corrupt_data.csv")

display(df1)

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/corrupt_data-1.csv')

# COMMAND ----------

