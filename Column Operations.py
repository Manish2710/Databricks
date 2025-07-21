# Databricks notebook source
employee = [(10, "Michal", "Robinson", "1999", "100", "M", 2000),
        (20, "James", "Wood", "2003", "200", "F", 8000),
        (30, "Chris", "Andrews", "2005", "100", None, 6000),
        (40, "Mark", "Bond", "2008", "100", "F", 7000),
        (50, "Steve", "Watson","1996", "400", "M", 1000),
        (60, "Mathews", "Simon", "1998", "500", "M", 5000),
        (70, "Peter", "Paul", "2011", "600", "M", 5000)]

Schema = ["employee_id", "FirstName", "LastName", "DoJ", "Employee_Dept_ID", "Gender", "Salary"]
empDF = spark.createDataFrame(data = employee, schema = Schema)
display(empDF)

# COMMAND ----------

from pyspark.sql.functions import lit

empDF_AddColumn = empDF.withColumn("Location", lit("Mumbai")).show()

# COMMAND ----------

from pyspark.sql.functions import concat

empDF_AddColumn = empDF.withColumn("Bonus", empDF.Salary *0.1).withColumn("Name", concat("FirstName",lit(" "), "LastName"))

display(empDF_AddColumn)

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename a Column**

# COMMAND ----------

empDF_RenameColumn = empDF_AddColumn.withColumnRenamed("Name", "Full_Name").withColumnRenamed("DoJ", "Date_of_Joining").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop a Column**

# COMMAND ----------

empDF_DropColumn = empDF_AddColumn.drop("Name").drop("Bonus").show()

# COMMAND ----------

