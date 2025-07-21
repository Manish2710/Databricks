# Databricks notebook source
empDF = [('John', [4, 6, 7, 9, 2], [1, 2, 3, 7, 7]), ('David', [7, 5, 1, 4, 7, 1], [3, 2, 8, 9, 4, 9]), ('Mike', [3, 9, 1, 6, 2], [1, 2, 3, 5, 8])]
df = spark.createDataFrame(data=empDF, schema = ['Name', 'Array_1', 'Array_2'])
df.show()

# COMMAND ----------

from pyspark.sql import functions as F

df_intersect = df.withColumn("Except", F.array_except(df.Array_1, df.Array_2))

df_intersect.show()

# COMMAND ----------

