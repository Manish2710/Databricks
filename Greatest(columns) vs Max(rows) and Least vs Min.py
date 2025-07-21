# Databricks notebook source
input_data = [
    ("David", 70, 68, 89, 40, 84),
    ("Kevin", 90, 67, 87, 79, 74),
    ("Natalia", 66, 88, 45, 65, 72),
    ("Roger", 78, 73, 62, 49, 67),
    ("Michael", 80, 86, 69, 78, 92)
]

schema = ["Student", "Subject_1", "Subject_2", "Subject_3", "Subject_4", "Subject_5"]

df = spark.createDataFrame(input_data, schema)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Greatest of Columns**

# COMMAND ----------

from pyspark.sql.functions import greatest

greatDF = df.withColumn("Greatest", greatest("Subject_1", "Subject_2", "Subject_3", "Subject_4", "Subject_5"))

greatDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Least of Columns**

# COMMAND ----------

from pyspark.sql.functions import least

leastDF = df.withColumn("least", least("Subject_1", "Subject_2", "Subject_3", "Subject_4", "Subject_5"))

leastDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Max of Rows**

# COMMAND ----------

df.agg({'Subject_1': 'max'}).display()

df.agg({'Subject_1': 'max', 'Subject_2': 'max'}).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Min of Rows**

# COMMAND ----------

df.agg({'Subject_1': 'min'}).display()

df.agg({'Subject_1': 'min', 'Subject_2': 'min'}).display()

# COMMAND ----------

