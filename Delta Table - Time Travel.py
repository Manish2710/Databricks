# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE scd2Demo (
# MAGIC   pk1 INT,
# MAGIC   pk2 STRING,
# MAGIC   dim1 INT,
# MAGIC   dim2 INT,
# MAGIC   dim3 INT,
# MAGIC   dim4 INT,
# MAGIC   active_status STRING,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP)
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/tables/scd2Demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from scd2Demo

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history scd2Demo

# COMMAND ----------

# MAGIC %md
# MAGIC **PySpark Approaches**

# COMMAND ----------

# MAGIC %md
# MAGIC **Pyspark - Timestamp + Table**

# COMMAND ----------

df = spark.read.format("delta").option('timestampAsOf', '2025-06-21T11:20:53.000+0000').table("scd2Demo")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pyspark - Timestamp + Path**

# COMMAND ----------

df = spark.read.format("delta").option('timestampAsOf', '2025-06-21T11:20:53.000+0000').load("/FileStore/tables/scd2Demo")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pyspark - Version + Path**

# COMMAND ----------

df = spark.read.format("delta").option('versionAsOF', 3).load("/FileStore/tables/scd2Demo")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pyspark - Version + Table**

# COMMAND ----------

df = spark.read.format("delta").option('versionAsOF', 3).table("scd2Demo")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL Approaches**

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL - Version + Table**

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from scd2Demo version as of 2;

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL - Version + Path**

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from delta.`/FileStore/tables/scd2Demo` version as of 2;

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL - Timestamp + Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from scd2Demo timestamp as of "2025-06-21T11:20:53.000+0000"

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL - Timestamp + Path**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta.`/FileStore/tables/scd2Demo` timestamp as of "2025-06-21T11:20:53.000+0000"

# COMMAND ----------

