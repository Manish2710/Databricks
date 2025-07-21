# Databricks notebook source
dbutils.fs.mkdirs('/FileStore/tables/babies/baby_names_input/')

dbutils.fs.mkdirs('/FileStore/tables/babies/baby_names_output/')

# COMMAND ----------

# MAGIC %md
# MAGIC **Upload Files**

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/babies/baby_names_input/')

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Sample DataFrame**

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").load("/FileStore/tables/babies/baby_names_input/")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Distinct Year List and Count for each Year**

# COMMAND ----------

df.groupBy("Year").count().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **PartitionBy One key Column**

# COMMAND ----------

df.write.option("header", True).partitionBy("Year").mode("overwrite").csv("/FileStore/tables/babies/baby_names_output")

# COMMAND ----------

# MAGIC %md
# MAGIC **PartitionBY Multiple Key Columns**

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/babies/baby_names_output/', True)

dbutils.fs.mkdirs('/FileStore/tables/babies/baby_names_output/')

# COMMAND ----------

df.write.option("header", True).partitionBy("Year", "Sex").mode("overwrite").csv("/FileStore/tables/babies/baby_names_output")

# COMMAND ----------

# MAGIC %md
# MAGIC **PartitionBy Key Column along with maximum number of records for each position**

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/babies/baby_names_output/', True)

dbutils.fs.mkdirs('/FileStore/tables/babies/baby_names_output/')

# COMMAND ----------

df.write.option("header", True).option("maxRecordsPerFile", 4200).partitionBy("Year").mode("overwrite").csv("/FileStore/tables/babies/baby_names_output")

# COMMAND ----------

