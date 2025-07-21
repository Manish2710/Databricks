# Databricks notebook source
# MAGIC %md
# MAGIC **Delta table instance is replica of delta table.
# MAGIC - It is mainly used to perform DML operations on delta table using Pyspark language
# MAGIC - It can be created using 2 ways**

# COMMAND ----------

# MAGIC %md
# MAGIC **Approach 1:Using forPath**

# COMMAND ----------

# MAGIC %md
# MAGIC **Approach 2: Using forName**

# COMMAND ----------

# MAGIC %md
# MAGIC Syntax
# MAGIC - deltaTable = DeltaTable.forPath(spark, "/path/to/table")
# MAGIC - deltaTable = DeltaTable.forName(spark, "table_name")

# COMMAND ----------

from delta.tables import * 

DeltaTable.create(spark).tableName("employee_demo1").addColumn("emp_id", "INT").addColumn("emp_name", "STRING").addColumn("gender", "STRING").addColumn("salary", "INT").addColumn("dept", "STRING").property("description", "table created for demo purpose").location("/FileStore/tables/delta/Employee").execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into employee_demo1 values(100, "Stephen", "M", 2000, "IT");
# MAGIC insert into employee_demo1 values(200, "Philipp", "M", 8000, "HR");
# MAGIC insert into employee_demo1 values(300, "Lara", "F", 6000, "SALES");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

deltaInstance1 = DeltaTable.forPath(spark, "/FileStore/tables/delta/Employee")

# COMMAND ----------

display(deltaInstance1.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from employee_demo1 where emp_id = 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

deltaInstance1.delete("emp_id = 200")

# COMMAND ----------

display(deltaInstance1.toDF())

# COMMAND ----------

deltaInstance2 = DeltaTable.forName(spark, "employee_demo1")

# COMMAND ----------

display(deltaInstance2.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history employee_demo1

# COMMAND ----------

display(deltaInstance2.history())

# COMMAND ----------

