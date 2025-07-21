# Databricks notebook source
data_student = [("Raja","Science",80,"P",90),
                ("Rakesh","Maths",90,"P",70),
                ("Rama","English",20,"F",80),
                ("Ramesh","Science",8,"F",75),
                ("Rajesh","Maths",30,"F",50),
                ("Raghav","Maths", None, "NA",70)]
Schema = ["name","Subject","Mark","Status","Attendance"]
df = spark.createDataFrame(data=data_student, schema = Schema)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC **Updating the Existing Column**

# COMMAND ----------

from pyspark.sql.functions import when

df1 = df.withColumn("Status", when(df.Mark >= 50, "Pass").when(df.Mark < 50, "Fail").otherwise("Absentee"))

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create a new column**

# COMMAND ----------

from pyspark.sql.functions import when

df2 = df.withColumn("New_Status", when(df.Mark >= 50, "Pass").when(df.Mark < 50, "Fail").otherwise("Absentee"))

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Another Syntax method**

# COMMAND ----------

from pyspark.sql.functions import expr

df3 = df.withColumn("new_Status", expr("CASE WHEN mark >= 50 THEN 'PASS' " + "WHEN Mark < 50 THEN 'Fail' " + "ELSE 'Absentee' END"))

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Multiple conditions using AND and OR Operators**

# COMMAND ----------

from pyspark.sql.functions import when

df4 = df.withColumn("Grade", when((df.Mark >= 80) &(df.Attendance >= 80), "Distinction").when((df.Mark >= 50) & (df.Attendance >= 50), "Good").otherwise("Average"))

display(df4)

# COMMAND ----------

