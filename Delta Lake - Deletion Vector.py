# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Define schema
schema = StructType([
    StructField("ProductID", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Price", FloatType(), True),
    StructField("StockQuantity", IntegerType(), True)
])

# Define data
data = [
    ("P001", "Laptop", "Electronics", 999.99, 50),
    ("P002", "Smartphone", "Electronics", 699.99, 150),
    ("P003", "Tablet", "Electronics", 299.99, 100),
    ("P004", "Smartwatch", "Electronics", 199.99, 75),
    ("P005", "Headphones", "Accessories", 149.99, 200),
    ("P006", "Camera", "Electronics", 499.99, 60),
    ("P007", "Printer", "Electronics", 299.99, 40),
    ("P008", "Monitor", "Electronics", 199.99, 80),
    ("P009", "Keyboard", "Accessories", 49.99, 150),
    ("P010", "Mouse", "Accessories", 29.99, 200),
    ("P011", "Router", "Networking", 79.99, 90),
    ("P012", "Speaker", "Audio", 99.99, 110),
    ("P013", "Webcam", "Accessories", 59.99, 130),
    ("P014", "Charger", "Accessories", 19.99, 170),
    ("P015", "Power Bank", "Accessories", 39.99, 120),
    ("P016", "USB Drive", "Storage", 24.99, 250),
    ("P017", "External HDD", "Storage", 79.99, 180),
    ("P018", "Memory Card", "Storage", 19.99, 140),
    ("P019", "Graphics Card", "Components", 249.99, 30),
    ("P020", "Microphone", "Audio", 59.99, 95)
]

df = spark.createDataFrame(data, schema)

df.display()

# COMMAND ----------

df.repartition(1).write.format("delta").mode("overwrite").save("/tmp/delta/product_catalog")

# COMMAND ----------

spark.sql("CREATE TABLE product_catalog USING DELTA LOCATION '/tmp/delta/product_catalog'")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM product_catalog

# COMMAND ----------

# MAGIC %fs ls /tmp/delta/product_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY product_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM product_catalog WHERE ProductID='P004'

# COMMAND ----------

# MAGIC %fs ls /tmp/delta/product_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY product_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE product
# MAGIC TBLPROPERTIES ('delta.enableDeletionVectors' = true)
# MAGIC AS
# MAGIC SELECT * FROM product_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM PRODUCT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL PRODUCT

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM PRODUCT WHERE ProductID = 'P003'

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORy PRODUCT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE PRODUCT

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/product

# COMMAND ----------

# MAGIC %md
# MAGIC **Clean up Old Files**

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "False")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC VACUUM PRODUCT RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/product

# COMMAND ----------

