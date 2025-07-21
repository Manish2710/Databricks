# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

simpleData = [(100, "Mobile", 20000, 10), \
              (200, "Laptop", 85000, 12), \
              (300, "Television", 45000, 8), \
              (400, "Monitor", 25000, 9), \
              (500, "Headset", 25000, 15)]

defSchema = StructType([
    StructField("Product_id", IntegerType(), False), \
    StructField("Product_name", StringType(), True), \
    StructField("UnitPrice", IntegerType(), True), \
    StructField("DiscountPercent", IntegerType(), True)
])

df = spark.createDataFrame(data = simpleData, schema = defSchema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **CREATE_MAP - Convert Columns to Dictionary**

# COMMAND ----------

from pyspark.sql.functions import col, lit, create_map

dfDict = df.select(col("Product_id"), col("Product_name"), col("UnitPrice"), col("DiscountPercent"), create_map(col("Product_name"), col("UnitPrice")).alias("PrcieDict"))

dfDict.display()

# COMMAND ----------

from pyspark.sql.functions import col, lit, create_map

dfDict = df.withColumn("PriceDict", create_map(lit("Product_name"), col("Product_name"), lit("UnitPrice"), col("UnitPrice")))

dfDict.display()

# COMMAND ----------

from pyspark.sql.functions import col, lit, create_map

dfDict = df.withColumn("PriceDict", create_map(lit("Product_name"), col("Product_name"), lit("UnitPrice"), col("UnitPrice"), lit("DiscountPercent"), col("DiscountPercent")))

dfDict.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

dfDict.printSchema()

# COMMAND ----------

