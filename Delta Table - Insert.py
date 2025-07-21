# Databricks notebook source
dbutils.fs.rm("/FileStore/tables/delta/Employee", True)

# COMMAND ----------

from delta.tables import * 

DeltaTable.create(spark).tableName("employee_demo1").addColumn("emp_id", "INT").addColumn("emp_name", "STRING").addColumn("gender", "STRING").addColumn("salary", "INT").addColumn("dept", "STRING").property("description", "table created for demo purpose").location("/FileStore/tables/delta/Employee").execute()

# COMMAND ----------

# MAGIC %md
# MAGIC **Insert Using SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into employee_demo1 values(100, "Stephen", "M", 2000, "IT")

# COMMAND ----------

display(spark.sql("select * from employee_demo1"))

# COMMAND ----------

# MAGIC %md
# MAGIC **DataFrame Insert**

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType
employee_data = [(200, "Phillip", "M", 8000,"HR")]

employee_schema = StructType([StructField("emp_id", IntegerType(), False),
                              StructField("emp_name", StringType(), True),
                              StructField("gender", StringType(), True),
                              StructField("salary", IntegerType(), True),
                              StructField("dept", StringType(), True)
                              ])

df = spark.createDataFrame(data = employee_data, schema=employee_schema)

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("employee_demo1")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

# MAGIC %md
# MAGIC **DataFrame InsertInto Method**

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType
employee_data = [(300, "Lara", "F", 6000,"Sales")]

employee_schema = StructType([StructField("emp_id", IntegerType(), False),
                              StructField("emp_name", StringType(), True),
                              StructField("gender", StringType(), True),
                              StructField("salary", IntegerType(), True),
                              StructField("dept", StringType(), True)
                              ])

df1 = spark.createDataFrame(data = employee_data, schema=employee_schema)

display(df1)

# COMMAND ----------

df1.write.insertInto('employee_demo1', overwrite=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

# MAGIC %md
# MAGIC **Insert using Temp View**

# COMMAND ----------

df1.createOrReplaceTempView('delta_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into employee_demo1
# MAGIC select * from delta_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo1

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark SQL Insert**

# COMMAND ----------

spark.sql("insert into employee_demo1 select * from delta_data")

# COMMAND ----------

