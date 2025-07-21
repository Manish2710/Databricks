# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Sample data
data = [
    (1, "Alice", "Engineering", 80000),
    (2, "Bob", "Engineering", 75000),
    (3, "Charlie", "HR", 65000),
    (4, "David", "HR", 64000),
    (5, "Eva", "Sales", 72000),
    (6, "Frank", "Sales", 70000),
    (7, "Grace", "Engineering", 77000),
    (8, "Hank", "Sales", 71000),
    (9, "Ivy", "HR", 66000),
    (10, "Jack", "Engineering", 82000),
]

# Define schema
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
])

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Register the DataFrame as a SQL table
df.createOrReplaceTempView("employees")

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT department, count(*) AS employee_count
# MAGIC FROM employees
# MAGIC GROUP BY department
# MAGIC ORDER BY employee_count DESC

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

result_df = _sqldf

display(result_df)

top_departments = result_df.filter(result_df.employee_count > 3)
top_departments.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM employees

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM _sqldf

# COMMAND ----------

