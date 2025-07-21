# Databricks notebook source
# MAGIC %md
# MAGIC **Spark Version**

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()
print(spark.sparkContext.version)

# COMMAND ----------

employee_data = [
    (100, "Stephen", "1999", "100", "M", 2000),
    (200, "Philipp", "2002", "200", "M", 8000),
    (300, "John", "2010", "100", "M", 6000)
]
employee_schema = ["employee_id", "name", "doj", "employee_dept_id", "gender", "salary"]

df1 = spark.createDataFrame(data=employee_data, schema=employee_schema)
display(df1)


# COMMAND ----------

employee_data = [
    (300, "John", "2010", "100", "M", 6000),
    (400, "Nancy", "2004", "400", "F", 8000),
    (500, "Rosy", "2014", "500", "F", 9000)
]
employee_schema = ["employee_id", "name", "doj", "employee_dept_id", "gender", "salary"]

df2 = spark.createDataFrame(data=employee_data, schema=employee_schema)
display(df2)


# COMMAND ----------

# MAGIC %md
# MAGIC **Union**

# COMMAND ----------

df_union = df1.union(df2)

display(df_union)

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop Duplicates**

# COMMAND ----------

display(df_union.dropDuplicates())

# COMMAND ----------

# MAGIC %md
# MAGIC **UnionAll**

# COMMAND ----------

df_unionAll = df1.unionAll(df2)

display(df_unionAll)

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Mismatch**

# COMMAND ----------

df3 = df2.select(df2.employee_id, df2.name, df2.doj, df2.employee_dept_id, df2.gender)

display(df3)

# COMMAND ----------

df_invalid = df1.union(df3)

# COMMAND ----------


