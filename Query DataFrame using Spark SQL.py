# Databricks notebook source
# MAGIC %md
# MAGIC **Sample DataFrame**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Define the schema for the DataFrame
schema = StructType([
    StructField("ProductID", IntegerType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("StockQuantity", IntegerType(), True)
])

# Create a list of rows
data = [
    (1, "Laptop", "Electronics", 999.99, 50),
    (2, "Smartphone", "Electronics", 699.99, 100),
    (3, "Headphones", "Electronics", 49.99, 200),
    (4, "Book", "Books", 19.99, 300),
    (5, "Tablet", "Electronics", 299.99, 75)
]

# Create a DataFrame
productDF = spark.createDataFrame(data, schema=schema)
display(productDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **Old Approach**

# COMMAND ----------

productDF.createOrReplaceTempView("v_product")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from v_product

# COMMAND ----------

# MAGIC %md
# MAGIC **New Approach**

# COMMAND ----------

sqlDF = spark.sql("select * from {table}", table = productDF)
sqlDF.display()

# COMMAND ----------

sqlDF = spark.sql("select {column} from {table}", column = productDF.ProductName, table = productDF)
display(sqlDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transform Dataframe using Spark SQL**

# COMMAND ----------

transformDf = spark.sql("select ProductID, concat(ProductName, Category), Price * StockQuantity as TotalCost from {table}", table = productDF)
display(transformDf)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transform Dataframe using PySpark**

# COMMAND ----------

from pyspark.sql.functions import col, concat, expr

transformDfNew = productDF.select(col("ProductID"), concat(col("ProductName"), col("Category")).alias("ProductNameCategory"), (expr("Price * StockQuantity")).alias("TotalCost"))

transformDfNew.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Join DataFrames**

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import expr
 
# Sample data for products
product_data = [
    Row(product_id=1, product_name="Laptop", unit_price=800),
    Row(product_id=2, product_name="Smartphone", unit_price=500),
    Row(product_id=3, product_name="Tablet", unit_price=300),
    Row(product_id=4, product_name="Desktop", unit_price=1000),
    Row(product_id=5, product_name="Printer", unit_price=200)
    ]
 
# Sample data for sales
sales_data = [
    Row(sale_id=101, product_id=1, quantity=5),
    Row(sale_id=102, product_id=2, quantity=8),
    Row(sale_id=103, product_id=1, quantity=3),
    Row(sale_id=104, product_id=3, quantity=6),
    Row(sale_id=105, product_id=4, quantity=2),
    Row(sale_id=106, product_id=1, quantity=7)
    ]

product_df = spark.createDataFrame(product_data)
sales_df = spark.createDataFrame(sales_data)

display(product_df)
display(sales_df)

# COMMAND ----------

joinDf = spark.sql("select * from {table1} a join {table2} b on a.{joiningKey} = b.{joiningKey} ", table1 = product_df, table2 = sales_df, joiningKey = product_df.product_id)

display(joinDf)

# COMMAND ----------

