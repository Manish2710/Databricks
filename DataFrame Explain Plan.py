# Databricks notebook source
# MAGIC %md
# MAGIC **Create Employee DataFrame**

# COMMAND ----------

employee_schema = ["employee_id","name","doj","dept_id","gender","salary"]
employee_data = [(10,"Raj","1999-01","100","M",2000),
                 (20,"Rahul","2000-01","100","M",6000),
                 (30,"Raghav","2010-01","100","M",8000),
                 (40,"Raja","2004-01","100","F",10000),
                 (50,"Rama","2005-01","100","F",11000),
                 (60,"Rasul","2014-01","100","M",5000)]
employeeDF = spark.createDataFrame(data=employee_data, schema = employee_schema)
display(employeeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Department Dataframe**

# COMMAND ----------

department_data = [("HR",100),("Supply",200), ("Sales", 300), ("Stock", 400)]
department_schema = ["dept_name", "dept_id"]

departmentDf = spark.createDataFrame(department_data, department_schema)
display(de)

# COMMAND ----------

# MAGIC %md
# MAGIC **Inner Join**

# COMMAND ----------

from pyspark.sql.functions import col

dfJoin = employeeDF.join(departmentDf, employeeDF.dept_id == departmentDf.dept_id, "inner").withColumn("bonus", col("salary") * 0.1).groupBy("dept_name").sum("salary")

display(dfJoin)

# COMMAND ----------

dfJoin.explain()

# COMMAND ----------

dfJoin.explain(extended = True)

# COMMAND ----------

dfJoin.explain(mode = "simple")

# COMMAND ----------

dfJoin.explain(mode = "extended")

# COMMAND ----------

dfJoin.explain(mode = "formatted")

# COMMAND ----------

dfJoin.explain(mode = "cost")

# COMMAND ----------


