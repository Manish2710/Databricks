# Databricks notebook source
df = spark.createDataFrame(sc.parallelize([['ABC', [1,2,3]], ['XYZ', [2, None, 4]], ['KLM', [8,7]], ['IJK', [5]]]), ["key", "value"])

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Split Array Values into separate columns**

# COMMAND ----------

df.select("key", df.value[0], df.value[1], df.value[2]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **How to Automate this Solution?**

# COMMAND ----------

from pyspark.sql.functions import size, col

dfSize = df.select("key", "value", size("value").alias("NoOfArrayElements"))
dfSize.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Get the maximum Size of all Arrays**

# COMMAND ----------

max_value = dfSize.agg({"NoOfArrayElements":"max"}).collect()[0][0]
print(max_value)

# COMMAND ----------

# MAGIC %md
# MAGIC **UDF to convert Array Elements into Columns**

# COMMAND ----------

#from pyspark.sql import functions as F
def arraySplitIntoColumns(df, maxElements):
  for i in range(maxElements):
    df = df.withColumn(f"new_col_{i}", df.value[i])
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC **Calling UDF**

# COMMAND ----------

dfOut = arraySplitIntoColumns(df, max_value)
display(dfOut)

# COMMAND ----------

