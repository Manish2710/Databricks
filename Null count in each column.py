# Databricks notebook source
data_student = [("Raja","Science",80,"P",90),
                ("Rakesh","Maths",90,"P",None),
                ("Rama","English",20,"F",80),
                ("Ramesh","Science",None,"F",None),
                ("Rajesh","Maths",None,None,50),
                (None,None, None,None,None)]
Schema = ["name","Subject","Mark","Status","Attendance"]
df = spark.createDataFrame(data=data_student, schema = Schema)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, count, when

result = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
display(result)

# COMMAND ----------

