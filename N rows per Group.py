# Databricks notebook source
data_student = [("Michael", "Physics", 80, "P", 90),
                ("Michael", "Chemistry", 67, "P", 90),
                ("Michael", "Mathematics", 78, "P", 90),
                ("Nancy", "Physics", 30, "F", 80),
                ("Nancy", "Chemistry", 59, "P", 80),
                ("Nancy", "Mathematics", 75, "P", 80),
                ("David", "Physics", 90, "P", 70),
                ("David", "Chemistry", 87, "P", 70),
                ("David", "Mathematics", 97, "P", 70),
                ("John", "Physics", 33, "F", 60),
                ("John", "Chemistry", 28, "F", 60),
                ("John", "Mathematics", 52, "P", 60),
                ("Blessy", "Physics", 89, "P", 75),
                ("Blessy", "Chemistry", 76, "P", 75),
                ("Blessy", "Mathematics", 63, "P", 75)]
                
Schema = ["name", "Subject", "Mark", "Status", "Attendance"]

df = spark.createDataFrame(data = data_student, schema = Schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create rank within each group of name**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

windowDept = Window.partitionBy("name").orderBy(col("Mark").desc())
df2 = df.withColumn("row", row_number().over(windowDept)).orderBy("name","row")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Get Top N rows per group of Name**

# COMMAND ----------

df3 = df2.filter(col("row") <= 1)
df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create rank within each group of subject**

# COMMAND ----------

windowDept = Window.partitionBy("Subject").orderBy(col("Mark").desc())

df4 = df.withColumn("row", row_number().over(windowDept)).orderBy("name", "row")
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Get Top N rows per Group of Subject**

# COMMAND ----------

df5 = df4.filter(col("row") <= 1)
df5.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reverse Rank to get Bottom N rows per Group**

# COMMAND ----------

windowDept = Window.partitionBy("Subject").orderBy(col("Mark"))

df6 = df.withColumn("row", row_number().over(windowDept)).orderBy("name", "row")
display(df6)

# COMMAND ----------

df7 = df6.filter(col("row") <= 1)
df7.display()

# COMMAND ----------

