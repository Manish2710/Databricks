# Databricks notebook source
dbutils.fs.rm("/FileStore/tables/delta/",recurse = True)

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark).tableName("delta_internal_demo").addColumn("emp_id","INT").addColumn("emp_name", "STRING").addColumn("gender", "STRING").addColumn("salary", "INT").addColumn("dept", "STRING").property("description", "table created for demo purpose").location("/FileStore/tables/delta/arch_demo").execute()

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000003.json

# COMMAND ----------

display(spark.read.format("delta").load("/FileStore/tables/delta/arch_demo/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_internal_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into delta_internal_demo values(100, "Stephen", "M", 2000, "IT")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into delta_internal_demo values(200, "Philipp", "M", 8000, "HR");
# MAGIC insert into delta_internal_demo values(300, "Lara", "F", 6000, "SALES");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into delta_internal_demo values(100, "Stephen", "M", 2000, "IT");
# MAGIC insert into delta_internal_demo values(200, "Philipp", "M", 8000, "HR");
# MAGIC insert into delta_internal_demo values(300, "Lara", "F", 6000, "SALES");
# MAGIC
# MAGIC insert into delta_internal_demo values(100, "Stephen", "M", 2000, "IT");
# MAGIC insert into delta_internal_demo values(200, "Philipp", "M", 8000, "HR");
# MAGIC insert into delta_internal_demo values(300, "Lara", "F", 6000, "SALES");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_internal_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from delta_internal_demo where emp_id = 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_internal_demo

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000010.json

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log

# COMMAND ----------

display(spark.read.format("parquet").load("/FileStore/tables/delta/arch_demo/_delta_log/00000000000000000010.checkpoint.parquet"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete table delta_internal table

# COMMAND ----------

