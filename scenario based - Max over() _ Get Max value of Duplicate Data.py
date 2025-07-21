# Databricks notebook source
# MAGIC %md
# MAGIC **Max Over Window Function**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

simpleData = ((100, "Mobile", 5000, 10), \
              (100, "Mobile", 7000, 7), \
              (200, "Laptop", 20000, 4), \
              (200, "Laptop", 25000, 8), \
              (200, "Laptop", 25000, 8), \
              (200, "Laptop", 22000, 10))

defSchema = StructType([
    StructField("Product_id", IntegerType(), False), \
    StructField("Product_name", StringType(), True), \
    StructField("Price", IntegerType(), True), \
    StructField("DiscountPercent", IntegerType(), True)
])

df = spark.createDataFrame(data = simpleData, schema = defSchema)
display(df)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import max, col

windowSpec = Window.partitionBy('Product_id')

dfMax = (df.withColumn("maxprice", max('Price').over(windowSpec)).withColumn("maxDiscountPercent", max('DiscountPercent').over(windowSpec)))

display(dfMax)

# COMMAND ----------

# MAGIC %md
# MAGIC **Select Max Columns**

# COMMAND ----------

dfSel = dfMax.select(col("Product_id"), col("Product_name"), col("maxPrice").alias("Price"), col("maxDiscountPercent").alias("DiscountPercent"))
display(dfSel)

# COMMAND ----------

finalDf = dfSel.dropDuplicates()
display(finalDf)

# COMMAND ----------

