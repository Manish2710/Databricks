# Databricks notebook source
arrayData = [("John", 4,1), ("John", 6,2), ("David", 7,3), ("Mike", 3,4), ("David", 5,2), ("John", 7,3), ("John", 9,7), ("David",1,8), ("David", 4,9), ("David", 7,4), ("Mike", 8,5), ("Mike", 5,2), ("Mike", 3,8),  ("John", 2,7), ("David", 1,9)]

array_schema = ["Name", "Score_1", "Score_2"]

arrayDF = spark.createDataFrame(arrayData, array_schema)

display(arrayDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **Convert DataFrame to Array DataFrame**

# COMMAND ----------

from pyspark.sql import functions as F

masterDF = arrayDF.groupBy("Name").agg(F.collect_list("Score_1").alias("Array_Score_1"), F.collect_list("Score_2").alias("Array_Score_2"))

display(masterDF)
masterDF.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply arrays_zip function on Array DF**

# COMMAND ----------

arr_zip_df = masterDF.withColumn("Zipped_value", F.arrays_zip("Array_Score_1", "Array_Score_2"))

arr_zip_df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Realtime Example**

# COMMAND ----------

# Create Sample Dataframe
empDF = [
    ('Sales_dept', [{'emp_name':'John', 'salary':'1000', 'yrs_of_service':'10', 'Age':'33'},
                    {'emp_name':'David','salary':'2000','yrs_of_service':'15','Age':'40'},
                    {'emp_name':'Nancy','salary':'3000','yrs_of_service':'20','Age':'45'},
                    {'emp_name':'Rosy','salary':'4000','yrs_of_service':'8','Age':'30'},
                    {'emp_name':'Sami','salary':'6000','yrs_of_service':'5','Age':'31'}]),

    ('HR_dept', [{'emp_name':'Edwin','salary':'5000','yrs_of_service':'14','Age':'33'},
                 {'emp_name':'Tomas','salary':'3000','yrs_of_service':'12','Age':'42'},
                 {'emp_name':'Sarah','salary':'1500','yrs_of_service':'22','Age':'43'},
                 {'emp_name':'Stella','salary':'12000','yrs_of_service':'8','Age':'52'},
                 {'emp_name':'Kevin','salary':'4000','yrs_of_service':'5','Age':'27'}])
]

df_brand = spark.createDataFrame(data=empDF, schema=['Department', 'Employee'])
df_brand.printSchema()
display(df_brand)

# COMMAND ----------

# MAGIC %md
# MAGIC **Arrays_zip**

# COMMAND ----------

df_brandZip = df_brand.withColumn("Zip", F.arrays_zip(df_brand.Employee))

display(df_brandZip)

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply Explode**

# COMMAND ----------

df_brand_exp = df_brandZip.withColumn("Explode", F.explode(df_brandZip.Zip))

display(df_brand_exp)

# COMMAND ----------

# MAGIC %md
# MAGIC **Flatten Fields from Exploded list**

# COMMAND ----------

df_brand_output = df_brand_exp.withColumn("employee_emp_name", df_brand_exp["Explode.Employee.emp_name"]).withColumn("employee_yrs_of_service", df_brand_exp["Explode.Employee.yrs_of_service"]).withColumn("employee_salary", df_brand_exp["Explode.Employee.salary"]).withColumn("employee_Age", df_brand_exp["Explode.Employee.Age"]).drop("Explode").drop("zip").drop("Employee")

# COMMAND ----------

display(df_brand_output)

# COMMAND ----------

