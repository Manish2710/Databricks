# Databricks notebook source
dbutils.fs.rm("/FileStore/tables/delta/Employee", True)

# COMMAND ----------

from delta.tables import * 

DeltaTable.create(spark).tableName("employee_demo1").addColumn("emp_id", "INT").addColumn("emp_name", "STRING").addColumn("gender", "STRING").addColumn("salary", "INT").addColumn("dept", "STRING").property("description", "table created for demo purpose").location("/FileStore/tables/delta/Employee").execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into employee_demo1 values(100, "Stephen", "M", 2000, "IT");
# MAGIC insert into employee_demo1 values(200, "Philipp", "M", 8000, "HR");
# MAGIC insert into employee_demo1 values(300, "Lara", "F", 6000, "SALES");
# MAGIC insert into employee_demo1 values(400, "Mike", "M", 4000, "IT");
# MAGIC insert into employee_demo1 values(500, "Sarah", "F", 9000, "HR");
# MAGIC insert into employee_demo1 values(600, "Serena", "F", 5000, "SALES");
# MAGIC insert into employee_demo1 values(700, "Mark", "M", 6000, "SALES");

# COMMAND ----------

# MAGIC %md
# MAGIC **Using SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from employee_demo1 where emp_id = 100;

# COMMAND ----------

# MAGIC %md
# MAGIC **Using SQL using Delta Location**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from delta."/FileStore/tables/delta/Employee" where emp_id = 200

# COMMAND ----------

# MAGIC %md
# MAGIC **Using Spark SQL**

# COMMAND ----------

spark.sql("delete from employee_demo1 where emp_id = 300")

# COMMAND ----------

# MAGIC %md
# MAGIC **Using PySpark Delta table Instance**

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forName(spark, 'employee_demo1')

deltaTable.delete("emp_id = 400")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

# MAGIC %md
# MAGIC **Multiple Conditions using SQL Predicate**

# COMMAND ----------

deltaTable.delete("emp_id = 500 and gender = 'F'")

# COMMAND ----------

# MAGIC %md
# MAGIC **PySpark Delta Table Instance - Spark SQL Predicate**

# COMMAND ----------

deltaTable.delete(col('emp_id') == 600)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

