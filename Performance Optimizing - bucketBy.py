# Databricks notebook source
# MAGIC %md
# MAGIC **Check if Bucketing Enabled**

# COMMAND ----------

spark.conf.get("spark.sql.sources.bucketing.enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create sample data for Demo**

# COMMAND ----------

from pyspark.sql.functions import col, rand

df = spark.range(1, 10000, 1, 10).select(col("id").alias("PK"), rand(10).alias("Attribute"))

df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Non-Bucketed Table**

# COMMAND ----------

df.write.format("parquet").saveAsTable("nonbucketedTable")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Bucketed Table**

# COMMAND ----------

df.write.format("parquet").bucketBy(10, "PK").saveAsTable("bucketedTable")

# COMMAND ----------

# MAGIC %md
# MAGIC **Created Bucketed and Non-Bucketed Dataframes for Demo**

# COMMAND ----------

df1 = spark.table("bucketedTable")
df2 = spark.table("bucketedTable")

df3 = spark.table("nonbucketedTable")
df4 = spark.table("nonbucketedTable")

# COMMAND ----------

# MAGIC %md
# MAGIC **Broadcast Join by Default if less than 10 MB**

# COMMAND ----------

df3.join(df4, "PK", "inner").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **Disable Broadcast Join**

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", False)

# COMMAND ----------

display(df3.join(df4, "PK", "inner"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Non-bucketed to non-bucketed join. Both sides would be shuffled**

# COMMAND ----------

df3.join(df4, "PK", "inner").explain()

# COMMAND ----------

df3.join(df1, "PK").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Non-bucketed to bucketed join. One side would be shuffled**

# COMMAND ----------

df3.join(df1, "PK").explain()

# COMMAND ----------

df1.join(df2, "PK").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Bucketed to bucketed join. No shuffle at both sides**

# COMMAND ----------

df1.join(df2, "PK").explain()

# COMMAND ----------

