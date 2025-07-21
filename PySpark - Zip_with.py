# Databricks notebook source
# MAGIC %md
# MAGIC **Sample DataFrame**

# COMMAND ----------

from pyspark.sql.functions import col, zip_with

data = [
  (1, [1, 2, 3], [4, 5, 6]),
  (2, [7, 8, 6], [10, 11, 8]),
  (3, [13, 14, 15], [16, 17, 18])
]

schema = ["id", "array1", "array2"]

df = spark.createDataFrame(data, schema)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Zip_With**

# COMMAND ----------

add_elements = lambda x, y: x + y

df_with_zipped = df.withColumn("zipped", zip_with(col("array1"), col("array2"), add_elements))

display(df_with_zipped)

# COMMAND ----------

# MAGIC %md
# MAGIC **Inconsistent Number of Elements Across Different Rows**

# COMMAND ----------

from pyspark.sql.functions import col, zip_with

data = [
  (1, [1, 2, 3], [4, 5, 6]),
  (2, [7, 8], [10, 11]),
  (3, [13, 14, 15, 4], [16, 17, 18, 9])
]

schema = ["id", "array1", "array2"]

df = spark.createDataFrame(data, schema)

df.display()

# COMMAND ----------

add_elements = lambda x, y: x + y

df_with_zipped = df.withColumn("zipped", zip_with(col("array1"), col("array2"), add_elements))

display(df_with_zipped)

# COMMAND ----------

# MAGIC %md
# MAGIC **Inconsistent Number of Elements Across Same Row**

# COMMAND ----------

from pyspark.sql.functions import col, zip_with

data = [
  (1, [1, 2, 3], [4, 5, 6]),
  (2, [7, 8], [10, 11]),
  (3, [13, 14, 15, 4], [16, 17, 18, 9]), 
  (4, [6, 12], [3, 9, 6, 15])
]

schema = ["id", "array1", "array2"]

df = spark.createDataFrame(data, schema)

df.display()

# COMMAND ----------

add_elements = lambda x, y: x + y

df_with_zipped = df.withColumn("zipped", zip_with(col("array1"), col("array2"), add_elements))

display(df_with_zipped)

# COMMAND ----------

from pyspark.sql.types import ArrayType, IntegerType

def pad_arrays(arr1, arr2):
    max_length = max(len(arr1), len(arr2))
    padded_arr1 = arr1 + [0] * (max_length - len(arr1))
    padded_arr2 = arr2 + [0] * (max_length - len(arr2))

    return padded_arr1, padded_arr2


pad_arrays_udf = udf(pad_arrays, ArrayType(ArrayType(IntegerType())))

df_padded = df.withColumn("padded_arrays", pad_arrays_udf(col("array1"), col("array2")))

display(df_padded)

# COMMAND ----------

df_padded = df_padded.withColumn("padded_array1", col("padded_arrays")[0]).withColumn("padded_array2", col("padded_arrays")[1]).drop("padded_arrays")

df_padded.display()

# COMMAND ----------

add_elements = lambda x, y: x + y

df_with_zipped = df_padded.withColumn("zipped", zip_with(col("padded_array1"), col("padded_array2"), add_elements))

display(df_with_zipped)

# COMMAND ----------

