# Databricks notebook source
# MAGIC %md
# MAGIC **Using PySpark**

# COMMAND ----------

from delta.tables import *

#DeltaTable.create(spark).tableName("employee_demo").addColumn("emp_id", "INT").addColumn("emp_name", "STRING").addColumn("gender", "STRING").addColumn("salary", "INT").addColumn("dept", "STRING").property("description", "table created for demo purpose").location("/FileStore/tables/delta/createtable").execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo

# COMMAND ----------

#DeltaTable.createIfNotExists(spark).tableName("employee_demo").addColumn("emp_id", "INT").addColumn("emp_name", "STRING").addColumn("gender", "STRING").addColumn("salary", "INT").addColumn("dept", "STRING").property("description", "table created for demo purpose").location("/FileStore/tables/delta/createtable").execute()

# COMMAND ----------

#DeltaTable.createOrReplace(spark).tableName("employee_demo").addColumn("emp_id", "INT").addColumn("emp_name", "STRING").addColumn("gender", "STRING").addColumn("salary", "INT").addColumn("dept", "STRING").property("description", "table created for demo purpose").location("/FileStore/tables/delta/createtable").execute()

# COMMAND ----------

# MAGIC %md
# MAGIC **Using SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE employee_demo (
# MAGIC     emp_id INT,
# MAGIC     emp_name STRING,
# MAGIC     gender STRING,
# MAGIC     salary INT,
# MAGIC     dept STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS employee_demo (
# MAGIC     emp_id INT,
# MAGIC     emp_name STRING,
# MAGIC     gender STRING,
# MAGIC     salary INT,
# MAGIC     dept STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE employee_demo (
# MAGIC     emp_id INT,
# MAGIC     emp_name STRING,
# MAGIC     gender STRING,
# MAGIC     salary INT,
# MAGIC     dept STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC **Using Existing DataFrame**

# COMMAND ----------

employee_data = [(100,"Stephen","M",2000,"IT"), (200,"Philip","M",8000,"HR"), (300,"Lara","F",6000,"SALES")]

employee_schema = ["emp_id","emp_name","gender","salary","dept"]

df = spark.createDataFrame(data=employee_data, schema = employee_schema)
display(df)


# COMMAND ----------

df.write.format("delta").saveAsTable("employee_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo

# COMMAND ----------

