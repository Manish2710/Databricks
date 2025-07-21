# Databricks notebook source
dbutils.fs.rm('/FileStore/tables/Exercise1/AdventureWorks_Sales_200_Rows.csv')

# COMMAND ----------

from pyspark.sql import functions as F

root_path = "/FileStore/tables/Exercise1/"

df = spark.read.option("sep", "|").option("header", True).csv(root_path + "*")
display(df)

# COMMAND ----------

df1 = df.repartition(8)
print(df1.rdd.getNumPartitions()) 

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

df1 = df1.withColumn("partitionId", spark_partition_id())
df1.display()

# COMMAND ----------

df2 = df.repartition(20).withColumn("partitionID", spark_partition_id())
df2.display()

# COMMAND ----------

df2.rdd.getNumPartitions()

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id, asc, desc

df3 = df2.withColumn("partitionID", spark_partition_id()).groupBy("partitionID").count().orderBy(desc("count"))

df3.display()

# COMMAND ----------

df4 = df1.withColumn("partitionID", spark_partition_id()).groupBy("partitionID").count().orderBy(desc("count"))

df4.display()

# COMMAND ----------

