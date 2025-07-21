# Databricks notebook source
employee = [(10, "Michal Robinson", "1999-06-01", "100", 2000),
        (20, "James Wood", "2003-03-01", "200", 8000),
        (30, "Chris Andrews", "2005-04-01", "100", 6000),
        (40, "Mark Bond", "2008-10-01", "100", 7000),
        (50, "Steve Watson","1996-02-01", "400", 1000),
        (60, "Mathews Simon", "1998-11-01", "500", 5000),
        (70, "Peter Paul", "2011-04-01", "600", 5000)]

Schema = ["employee_id", "Name", "DoJ", "Employee_Dept_ID", "Salary"]
empDF = spark.createDataFrame(data = employee, schema = Schema)
display(empDF)

# COMMAND ----------

from pyspark.sql.functions import split

df1 = empDF.withColumn('First_Name', split(empDF.Name, ' ').getItem(0)).withColumn('Last_Name', split(empDF.Name, ' ').getItem(1))

display(df1)

# COMMAND ----------

import pyspark

split_col = pyspark.sql.functions.split(empDF.Name, ' ')

df2 = empDF.withColumn('First_Name', split_col.getItem(0)).withColumn('Last_Name', split_col.getItem(1))

display(df2)

# COMMAND ----------

split_col = pyspark.sql.functions.split(empDF.DoJ, '-')

df3 = empDF.select("employee_id", "Name", "Employee_Dept_ID", "Salary", split_col.getItem(0).alias('joining_year'),split_col.getItem(1).alias('joining_month'),split_col.getItem(2).alias('joining_day'))

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Combining multiple Splits**

# COMMAND ----------

df4 = empDF.withColumn('First_Name', split(empDF.Name, ' ').getItem(0)).withColumn('Last_Name', split(empDF.Name, ' ').getItem(1)).withColumn('Joining_year', split(empDF.DoJ, '-').getItem(0)).withColumn('Joining_year', split(empDF.DoJ, '-').getItem(1)).withColumn('Joining_year', split(empDF.DoJ, '-').getItem(2))

display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Split and Drop splitted columns**

# COMMAND ----------

df5 = empDF.withColumn('First_Name', split(empDF.Name, ' ').getItem(0)).withColumn('Last_Name', split(empDF.Name, ' ').getItem(1)).withColumn('Joining_year', split(empDF.DoJ, '-').getItem(0)).withColumn('Joining_year', split(empDF.DoJ, '-').getItem(1)).withColumn('Joining_year', split(empDF.DoJ, '-').getItem(2)).drop(empDF.Name).drop(empDF.DoJ)

display(df5)

# COMMAND ----------

