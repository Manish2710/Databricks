# Databricks notebook source
from delta.tables import *

DeltaTable.create(spark).tableName("employee_demo").addColumn("emp_id", "INT").addColumn("emp_name", "STRING").addColumn("gender", "STRING").addColumn("salary", "INT").addColumn("dept", "STRING").property("description", "table created for demo purpose").location("/FileStore/tables/delta/path_employee_demo").execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into employee_demo values(100, "Stephen", "M", 2000, "IT");

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Evolution**

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType

employee_data = [(200, "Philipp", "M", 8000, "HR", "test data")]

employee_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("emp_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("dept", StringType(), True),
    StructField("additionalcolumn1", StringType(), True)
])

df = spark.createDataFrame(data=employee_data, schema=employee_schema)

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

df.write.option("mergeSchema", True).format("delta").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType

employee_data = [(300, "David", "M", 8000, "HR", "dummy data")]

employee_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("emp_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("dept", StringType(), True),
    StructField("additionalcolumn2", StringType(), True)
])

df = spark.createDataFrame(data=employee_data, schema=employee_schema)

display(df)

# COMMAND ----------

df.write.option("mergeSchema", True).format("delta").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo

# COMMAND ----------

