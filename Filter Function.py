# Databricks notebook source
employee = [(10, "Michal Robinson", "1999", "100", "M", 2000),
        (20, "James Wood", "2003", "200", "F", 8000),
        (30, "Chris Andrews", "2005", "100", None, 6000),
        (40, "Mark Bond", "2008", "100", "F", 7000),
        (50, "Steve Watson","1996", "400", "M", 1000),
        (60, "Mathews Simon", "1998", "500", "M", 5000),
        (70, "Peter Paul", "2011", "600", "M", 5000)]

Schema = ["employee_id", "Name", "DoJ", "Employee_Dept_ID", "Gender", "Salary"]
empDF = spark.createDataFrame(data = employee, schema = Schema)
display(empDF)

# COMMAND ----------

display(empDF)

display(empDF.filter(empDF.Salary != 5000))  # ==, >, <, <=, >=, !=

# COMMAND ----------

display(empDF)

display(empDF.filter((empDF.Gender == "F") | (empDF.DoJ == "2003")))

# COMMAND ----------

display(empDF.filter((empDF.Gender == "F") & (empDF.DoJ == "2003")))

# COMMAND ----------

display(empDF.filter(empDF.Name.endswith("n")))

display(empDF.filter(empDF.Name.startswith("Mi")))

display(empDF.filter(empDF.Name.contains("ch")))

# COMMAND ----------

display(empDF.filter(empDF.Gender.isNull()))

display(empDF.filter(empDF.Gender.isNotNull()))

# COMMAND ----------

display(empDF.filter(empDF.Employee_Dept_ID.isin(100, 500)))

# COMMAND ----------

display(empDF.filter(empDF.Name.like("%e%")))

# COMMAND ----------

