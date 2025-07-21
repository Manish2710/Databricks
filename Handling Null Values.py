# Databricks notebook source
data_student = [("Michal", "Science", 80, "P", 90),
        ("Nancy", "Mathematics", 90, "P", None),
        ("David", "English", 20, "F", 80),
        ("John", "Science", None, "F", None),
        ("Blessy", None, 30, "F", 50),
        ("Martin", "Mathematics", None, None, 70)]

Schema = ["name", "Subject", "Mark", "Status", "Attendance"]
df = spark.createDataFrame(data = data_student, schema = Schema)
display(df)

# COMMAND ----------

display(df.filter(df.Mark.isNull()))

#display(df.filter("Mark IS NULL"))

#from pyspark.sql.functions import col
# display(df.filter(col("Mark").isNull()))

# COMMAND ----------

display(df.filter(df.Mark.isNotNull()))

display(df.filter((df.Mark.isNotNull()) & (df.Attendance.isNotNull())))

# COMMAND ----------

display(df.filter((df.Mark.isNotNull()) | (df.Attendance.isNotNull())))

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop the records with NULL Value - ALL & ANY**
# MAGIC

# COMMAND ----------

data_student = [("Michal", "Science", 80, "P", 90),
        ("Nancy", "Mathematics", 90, "P", None),
        ("David", "English", 20, "F", 80),
        ("John", "Science", None, "F", None),
        ("Blessy", None, 30, "F", 50),
        ("Martin", "Mathematics", None, None, 70),
        (None, None, None, None, None)]

Schema = ["name", "Subject", "Mark", "Status", "Attendance"]
df = spark.createDataFrame(data = data_student, schema = Schema)
display(df)

# COMMAND ----------

display(df.na.drop())

display(df.na.drop("any"))

display(df.dropna("any"))

# COMMAND ----------

display(df.na.drop("all"))

# COMMAND ----------

display(df.na.drop(subset=["Mark", "Attendance"]))

# COMMAND ----------

# MAGIC %md
# MAGIC **Fill Value for all columns if NULL is Present**

# COMMAND ----------

display(df.na.fill(value = 0))

display(df.na.fill(value = "NA"))

display(df.fillna(value = 0))

# COMMAND ----------

# MAGIC %md
# MAGIC **Fill Value for all specific columns if NULL is Present**

# COMMAND ----------

display(df.na.fill(value = 0, subset = ["Mark", "Attendance"]))

display(df.na.fill({"Name": "No_name", "Subject": "English", "Mark": 0, "Status": "NA", "Attendance": 50}))

# COMMAND ----------

