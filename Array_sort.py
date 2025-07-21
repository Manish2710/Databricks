# Databricks notebook source
empDF = [('John', [4, 6, 7, 9, 2]), ('David', [7, 5, 1, 4, 7, 1]), ('Mike', [3, 9, 1, 6, 2])]
df = spark.createDataFrame(data=empDF, schema = ['Name', 'Array_1'])
df.show()

# COMMAND ----------

from pyspark.sql import functions as F

df_intersect = df.withColumn("Sorted", F.array_sort(df.Array_1))

df_intersect.show()

# COMMAND ----------

