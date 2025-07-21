# Databricks notebook source
sc.defaultParallelism

# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(10), IntegerType())

df.rdd.getNumPartitions()

# COMMAND ----------

df.rdd.glom().collect()

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep",",").load("/FileStore/tables/baby_names/")

df.rdd.getNumPartitions()

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", 100000)
spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep",",").load("/FileStore/tables/baby_names/")

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC **Repartition**

# COMMAND ----------

from pyspark.sql.types import IntegerType

df = spark.createDataFrame(range(10), IntegerType())

df.rdd.glom().collect()

# COMMAND ----------

df1 = df.repartition(20)

df1.rdd.getNumPartitions()

# COMMAND ----------

df1.rdd.glom().collect()

# COMMAND ----------

df1 = df.repartition(2)

df1.rdd.getNumPartitions()

# COMMAND ----------

df1.rdd.glom().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **Coalesce**

# COMMAND ----------

df2 = df.coalesce(2)

df2.rdd.getNumPartitions()

df2.rdd.glom().collect()

# COMMAND ----------

