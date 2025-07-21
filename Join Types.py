# Databricks notebook source
employee = [(10, "Michal", "Robinson", "1999", "100", "M", 2000),
        (20, "James", "Wood", "2003", "200", "F", 8000),
        (30, "Chris", "Andrews", "2005", "100", None, 6000),
        (40, "Mark", "Bond", "2008", "100", "F", 7000),
        (50, "Steve", "Watson","1996", "400", "M", 1000),
        (60, "Mathews", "Simon", "1998", "500", "M", 5000)]

Schema = ["employee_id", "FirstName", "LastName", "DoJ", "Employee_Dept_ID", "Gender", "Salary"]
empDF = spark.createDataFrame(data = employee, schema = Schema)
display(empDF)

# COMMAND ----------

dept = [("HR", 100), ("Supply", 200), ("Sales", 300), ("Stock", 400)]

d_schema = ["dept_name", "dept_id"]

deptDf = spark.createDataFrame(dept, d_schema)

display(deptDf)

# COMMAND ----------

# MAGIC %md
# MAGIC **Inner Join**

# COMMAND ----------

df_inner = empDF.join(deptDf, empDF.Employee_Dept_ID == deptDf.dept_id, "inner")

df_inner.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Full Outer Join**

# COMMAND ----------

df_full_outer = empDF.join(deptDf, empDF.Employee_Dept_ID == deptDf.dept_id, "full")

df_full_outer.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Left Outter Join**

# COMMAND ----------

df_left_outer = empDF.join(deptDf, empDF.Employee_Dept_ID == deptDf.dept_id, "left")

display(df_left_outer)

# COMMAND ----------

# MAGIC %md
# MAGIC **Right Outer Join**

# COMMAND ----------

df_right_outer = empDF.join(deptDf, empDF.Employee_Dept_ID == deptDf.dept_id, "right")

display(df_right_outer)

# COMMAND ----------

# MAGIC %md
# MAGIC **Left Semi Join**

# COMMAND ----------

df_left_semi = empDF.join(deptDf, empDF.Employee_Dept_ID == deptDf.dept_id, "leftsemi")

display(df_left_semi)

# COMMAND ----------

# MAGIC %md
# MAGIC **Left Anti Join**

# COMMAND ----------

df_left_anti = empDF.join(deptDf, empDF.Employee_Dept_ID == deptDf.dept_id, "anti")

display(df_left_anti)

# COMMAND ----------

