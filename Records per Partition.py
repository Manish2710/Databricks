# Databricks notebook source
df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").load("/FileStore/tables/babies/baby_names_input/baby_names.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Number of Records in the DataFrame**

# COMMAND ----------

print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Default Partition count**

# COMMAND ----------

print(df.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC **Number of **

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

df.withColumn("PartitionID", spark_partition_id()).groupBy("PartitionID").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Repartition the DataFrame to 5**

# COMMAND ----------

df_5 = df.select(df.Year, df.State, df.Sex, df.Count).repartition(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Get Number of Partitions in Dataframe**

# COMMAND ----------

print(df)

# COMMAND ----------

df_5.withColumn("PartitionId", spark_partition_id()).groupBy("PartitionId").count().show()

# COMMAND ----------

