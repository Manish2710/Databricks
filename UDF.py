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

# MAGIC %md
# MAGIC **Define UDF to Rename Columns**

# COMMAND ----------

import pyspark.sql.functions as f

def renameColumns(rename_df):
  for column in rename_df.columns:
    new_column = "Col" +column
    rename_df = rename_df.withColumnRenamed(column, new_column)
  
  return rename_df

# COMMAND ----------

renamed_df = renameColumns(empDF)
display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **UDF to convert name into upper case**

# COMMAND ----------

from pyspark.sql.functions import upper, col

def upperCase_col(df):
  upper_DF = df.withColumn('name_upper', upper(df.Name))

  return upper_DF

# COMMAND ----------

up_Case_DF = upperCase_col(empDF)
display(up_Case_DF)

# COMMAND ----------

