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
# MAGIC **Create Iterable Table**

# COMMAND ----------

column_list = empDF.columns

print(column_list)

# COMMAND ----------

# MAGIC %md
# MAGIC **Join the String**

# COMMAND ----------

joinedString = ",".join(column_list)

print(joinedString)

# COMMAND ----------

# MAGIC %md
# MAGIC **Use Cases**

# COMMAND ----------

joinString = " AND ".join(list(map(lambda x: ("Target." + x + " = Source."+x), column_list)))

print(joinString)

# COMMAND ----------

updateString = ",".join(list(map(lambda x: ("Target." + x + " = Source."+x), column_list)))

print(updateString)

# COMMAND ----------

sourceinsertString = ",".join(list(map(lambda x: ("Source."+x), column_list)))

print(sourceinsertString)

# COMMAND ----------

