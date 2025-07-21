# Databricks notebook source
# MAGIC %md
# MAGIC **Sample DataFrame**

# COMMAND ----------

from pyspark.sql.functions import rand

# Sample data for product dimension
data = [
    (1, "ProductA", "Category1", "BrandX", "Supplier1", 100, 10.99, "2023-01-01", "2023-01-10", True),
    (2, "ProductB", "Category2", "BrandY", "Supplier2", 50, 15.49, "2023-01-02", "2023-01-12", True),
    (3, "ProductC", "Category1", "BrandX", "Supplier1", 75, 8.99, "2023-01-03", "2023-01-14", False),
    (4, "ProductD", "Category3", "BrandZ", "Supplier3", 200, 25.99, "2023-01-04", "2023-01-16", True),
    (5, "ProductE", "Category2", "BrandY", "Supplier2", 60, 12.99, "2023-01-05", "2023-01-18", True)
]

# Define column names
columns = [
    "product_id", "product_name", "category", "brand", "supplier",
    "stock_quantity", "price", "start_date", "end_date", "active"
]

# Create the DataFrame
df = spark.createDataFrame(data, columns)

# Show the resulting DataFrame
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Convert DataFrame to view for SQL Operations**

# COMMAND ----------

df.createOrReplaceTempView("Products")

# COMMAND ----------

# MAGIC %md
# MAGIC **Traditional Approach**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT PRODUCT_ID, PRODUCT_NAME, CATEGORY, BRAND, SUPPLIER, STOCK_QUANTITY, PRICE, START_DATE, END_DATE, ACTIVE FROM PRODUCTS
# MAGIC
# MAGIC -- SELECT * FROM PRODUCTS

# COMMAND ----------

# MAGIC %md
# MAGIC **Project Selective Columns**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT PRODUCT_ID, PRODUCT_NAME, BRAND, SUPPLIER, STOCK_QUANTITY, PRICE, START_DATE, END_DATE FROM PRODUCTS

# COMMAND ----------

# MAGIC %md
# MAGIC **Best Approach using EXCEPT**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * EXCEPT (CATEGORY, ACTIVE) FROM PRODUCTS

# COMMAND ----------

# MAGIC %md
# MAGIC **Using Join**

# COMMAND ----------

# Sample data for the product dimension table
product_data = [
    (1, "ProductA", "BrandX"),
    (2, "ProductB", "BrandY"),
    (3, "ProductC", "BrandX"),
    (4, "ProductD", "BrandZ"),
    (5, "ProductE", "BrandY")
]

# Sample data for the customer table
customer_data = [
    (101, "Alice", 1, "2023-01-05"),
    (102, "Bob", 2, "2023-01-08"),
    (103, "Charlie", 1, "2023-01-12"),
    (104, "David", 3, "2023-01-15"),
    (105, "Eve", 4, "2023-01-20")
]

# Define column names for both tables
product_columns = ["product_id", "product_name", "brand"]
customer_columns = ["customer_id", "customer_name", "purchased_product_id", "purchase_date"]

product_df = spark.createDataFrame(product_data, product_columns)
customer_df = spark.createDataFrame(customer_data, customer_columns)

display(product_df)
display(customer_df)

product_df.createOrReplaceTempView("Product")
customer_df.createOrReplaceTempView("Customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT P.* EXCEPT (BRAND), C.* EXCEPT (CUSTOMER_ID) FROM PRODUCT P JOIN CUSTOMER C ON C.PURCHASED_PRODUCT_ID = P.PRODUCT_ID

# COMMAND ----------

# MAGIC %md
# MAGIC **Using PySpark**

# COMMAND ----------

display(df.select([col for col in df.columns if col not in {'category', 'active'}]))

# COMMAND ----------

