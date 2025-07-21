# Databricks notebook source
dbutils.fs.rm("/FileStore/tables/delta/Employee", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE employee_demo1 (
# MAGIC   emp_id INT,
# MAGIC   emp_name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING )
# MAGIC   USING DELTA
# MAGIC   LOCATION '/FileStore/tables/delta/Employee'

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

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL Standard Using Table Name**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC update employee_demo1 SET salary = 10000 where emp_name = "Mike";

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL Standard using Table Path**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC update delta.'/FileStore/tables/delta/Employee' set salary = 12000 emp_name = "Mark"

# COMMAND ----------

# MAGIC %md
# MAGIC **PySpark Standard using Table Instance**

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/FileStore/tables/delta/Employee')

deltaTable.update( condition = "emp_name = 'Mike'", set = {"salary":"6000"})

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

# MAGIC %md
# MAGIC **Using Spark SQL Functions**

# COMMAND ----------

deltaTable.update(condition=col('emp_name') == 'Mark', set = { 'dept': lit('IT')})

# COMMAND ----------

