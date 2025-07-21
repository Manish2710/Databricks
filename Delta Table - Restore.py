# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE scd2demo (
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
# MAGIC **Create Delta Table Instance**

# COMMAND ----------

from delta import *

targetTable = DeltaTable.forPath(spark, "/FileStore/tables/scd2Demo")

# COMMAND ----------

# MAGIC %md
# MAGIC **List down Version History**

# COMMAND ----------

display(targetTable.history())

# COMMAND ----------

# MAGIC %md
# MAGIC **Restore using Version Number**

# COMMAND ----------

targetTable.restoreToVersion(3)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from scd2demo

# COMMAND ----------

# MAGIC %md
# MAGIC **Restore using TimeStamp**

# COMMAND ----------

targetTable.restoreToTimestamp('2025-06-21T11:36:40.000+00:00')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from scd2demo

# COMMAND ----------

display(targetTable.history())

# COMMAND ----------

