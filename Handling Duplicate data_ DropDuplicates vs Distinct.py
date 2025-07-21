# Databricks notebook source
# MAGIC %md
# MAGIC **Create Sample Dataframe**

# COMMAND ----------

simpleData = ((111, "James", 8), \
              (222, "Mike", 5), \
              (111, "James", 8), \
              (222, "Mike", 12))

columns = ["id", "name", "score"]

df = spark.createDataFrame(data = simpleData, schema = columns)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Distinct to Remove Duplicate**

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Distinct with Subset**

# COMMAND ----------

df.select("id","name").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop Duplicate without Subset**

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop Duplicate with Subset**

# COMMAND ----------

df.dropDuplicates(['id', 'name']).display()

# COMMAND ----------

