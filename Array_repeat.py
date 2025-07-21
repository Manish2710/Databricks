# Databricks notebook source
# MAGIC %md
# MAGIC **Create Sample Delta Table**

# COMMAND ----------

empData = [
    (111, "Stephen", "King", 2000),
    (222, "Philipp", "Larkin", 8000),
    (333, "John", "Smith", 6000)
]

empSchema = ["Id", "FirstName", "LastName", "salary"]

df = spark.createDataFrame(data=empData, schema=empSchema)

display(df)

# COMMAND ----------

from pyspark.sql.functions import explode, array_repeat, col

repeatDf = df.withColumn("key_col", array_repeat(col("Id"), 5))
display(repeatDf)

# COMMAND ----------

df1 = df.withColumn("key_col")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lit, row_number

win = Window.orderBy(lit('A'))
df2 = df1.withColumn("RowN", row_number().over(win))
df3 = df2.withColumn("NewID", col("Id")+col("RowN"))
display(df3)

# COMMAND ----------

outputDf = df3.drop("Id", "key_col", "RowN").select(col("NewID").alias("Id"), "FirstName", "LastName", "Salary")
display(outputDf)

# COMMAND ----------

def multiplyInputData(df, keyCol, multiplyFactor):
    df1 = df.withColumn(f"{keyCol}", explode(array_repeat(col(f"{keyCol}"), multiplyFactor)))

    win = Window().orderBy(lit('A'))
    df2 = df1.withColumn("RowN", row_number().over(win))

    df3 = df2.withColumn(f"{keyCol}", col("Id")+col("RowN")).drop("RowN")
    return df3

# COMMAND ----------

finalDf = multiplyInputData(df, "Id", 10)
display(finalDf)

# COMMAND ----------

