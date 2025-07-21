# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([StructField("emp_id", IntegerType(), True), StructField("name", StringType(), True), StructField("city", StringType(), True), StructField("country", StringType(), True), StructField("contact_no", IntegerType(), True)])

# COMMAND ----------

data = [(1000, "Micheal", "Columbus", "USA", 689575210)]

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/delta_merge', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_employee (
# MAGIC   emp_id INT,
# MAGIC   name STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   contact_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/tables/delta_merge'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %md
# MAGIC **Using Spark SQL**

# COMMAND ----------

data = [(1000, "Micheal", "Columbus", "USA", 689575210), (2000, "Nancy", "New York", "USA", 958746320)]

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO dim_employee AS target
# MAGIC USING source_view AS source
# MAGIC ON target.emp_id = source.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.name = source.name,
# MAGIC     target.city = source.city,
# MAGIC     target.country = source.country,
# MAGIC     target.contact_no = source.contact_no
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (emp_id, name, city, country, contact_no)
# MAGIC   VALUES (source.emp_id, source.name, source.city, source.country, source.contact_no)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %md
# MAGIC **Using PySpark**

# COMMAND ----------

data = [(2000, "Sarah", "New York", "USA", 758264310), (3000, "David", "Atlanta", "USA", 564356787)]

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

from delta.tables import *

delta_df = DeltaTable.forPath(spark, "/FileStore/tables/delta_merge")

# COMMAND ----------

delta_df.alias("target").merge(
  source = df.alias("source"),
  condition = "target.emp_id = source.emp_id"
).whenMatchedUpdate(set = 
  {
    "name": "source.name",
    "city": "source.city",
    "country": "source.country",
    "contact_no": "source.contact_no"
  }
).whenNotMatchedInsert(values = 
  {
    "emp_id": "source.emp_id",
    "name": "source.name",
    "city": "source.city",
    "country": "source.country",
    "contact_no": "source.contact_no"
  }
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %md
# MAGIC **Audit Log**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table audit_log( operation string,
# MAGIC updated_time timestamp,
# MAGIC user_name string,
# MAGIC notebook_name string,
# MAGIC numTargetRowsUpdated int,
# MAGIC numTargetRowsInserted int,
# MAGIC numTargetRowsDeleted int
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from audit_log

# COMMAND ----------

display(delta_df.history())

# COMMAND ----------

# MAGIC %md
# MAGIC **Create DataFrame with Last Operation in Delta Table**

# COMMAND ----------

from delta.tables import *

delta_df = DeltaTable.forPath(spark, "/FileStore/tables/delta_merge")

lastOperationDf = delta_df.history(1)
display(lastOperationDf)

# COMMAND ----------

# MAGIC %md
# MAGIC **Explode Operation Metrics Column**

# COMMAND ----------

from pyspark.sql.functions import explode

explode_df = lastOperationDf.select(lastOperationDf.operation,explode(lastOperationDf.operationMetrics))

explode_df_select = explode_df.select(explode_df.operation, explode_df.key, explode_df.value.cast('int'))

display(explode_df_select)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pivot Operation to Convert Rows to Columns**

# COMMAND ----------

pivotDF = explode_df_select.groupBy("operation").pivot("key").sum("value")

display(pivotDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **Select only Columsn needed for our Audit Log Table**

# COMMAND ----------

pivotDFSelect = pivotDF.select(pivotDF.operation, pivotDF.numTargetRowsUpdated, pivotDF.numTargetRowsInserted, pivotDF.numTargetRowsDeleted)

display(pivotDFSelect)

# COMMAND ----------

# MAGIC %md
# MAGIC **Add Notebokk Parameters such as User Name, Notebook Path etc**

# COMMAND ----------

auditDF = pivotDFSelect.withColumn("user_name", lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())).withColumn("notebook_name", lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())).withColumn("updated_time", lit(current_timestamp()))

display(auditDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **Rearranging Columns in Dataframe to Match it with Audit Log Table**

# COMMAND ----------

auditDF_select = auditDF.select(auditDF.operation,auditDF.updated_time,auditDF.user_name,auditDF.notebook_name,auditDF.numTargetRowsUpdated,auditDF.numTargetRowsInserted,auditDF.numTargetRowsDeleted)

display(auditDF_select)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Temp View on DataFrame**

# COMMAND ----------

auditDF_select.createOrReplaceTempView("audit")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from audit

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from audit_log

# COMMAND ----------

# MAGIC %md
# MAGIC **Insert Audit Data into Audit Log Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into audit_log
# MAGIC select * from audit

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from audit_log

# COMMAND ----------

