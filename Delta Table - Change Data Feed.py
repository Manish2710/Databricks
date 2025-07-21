# Databricks notebook source
# MAGIC %md
# MAGIC **Create a silver table for PRODCUT**

# COMMAND ----------

products = [("USA", "ProductA", 1000, 200), ("India", "ProductB", 500, 50), ("UK", "ProductC", 700, 100), ("Canada", "ProductD", 200, 20)]
columns = ["Country", "Product", "Stock", "Sales"]

spark.createDataFrame(data=products, schema=columns).write.format("delta").mode("overwrite").saveAsTable("silver_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM silver_table

# COMMAND ----------

# MAGIC %md
# MAGIC **Generate gold table showing sales rates by country**

# COMMAND ----------

import pyspark.sql.functions as F

spark.read.format("delta").table("silver_table").withColumn("SalesRate", F.col("Sales") / F.col("Stock")).drop("Stock").drop("Sales").write.format("delta").mode("overwrite").saveAsTable("gold_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM gold_table

# COMMAND ----------

# MAGIC %md
# MAGIC **Enable change data feed on silver table**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE silver_table SET TBLPROPERTIES 

# COMMAND ----------

# MAGIC %md
# MAGIC **Update the Silver Table daily**

# COMMAND ----------

new_products = [("Australia", "productE", 300, 30)]

spark.createDataFrame(data=new_products, schema=columns).write.format("delta").mode("append").saveAsTable("silver_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE silver_table SET Stock = '1200' WHERE Country = 'USA' AND Product = 'ProductA'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM silver_table WHERE Country = 'UK' AND Product = 'ProductC'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM silver_table

# COMMAND ----------

# MAGIC %md
# MAGIC **Explore the changed data in SQL and PySpark**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY silver_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM table_changes('silver_table', 2) ORDER BY _commit_timestamp

# COMMAND ----------

changes_df = spark.read.format("delta").option("readChangeData", True).option("startingVersion", 2).table("silver_table")

display(changes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Propagate changes from silver to Gold table**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_table_latest_version AS
# MAGIC SELECT * 
# MAGIC FROM
# MAGIC (SELECT *, RANK() OVER (PARTITION BY Country, Product ORDER BY _commit_version DESC) AS rank FROM table_changes('silver_table', 2, 5) WHERE _change_type != 'update_preimage')
# MAGIC WHERE rank = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM silver_table_latest_version

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO gold_table t USING silver_table_latest_version s
# MAGIC ON s.Country = t.Country AND s.Product = t.Product
# MAGIC WHEN MATCHED AND s._change_type = 'update_postimage' THEN
# MAGIC UPDATE SET SalesRate = s.Sales/s.Stock
# MAGIC WHEN MATCHED AND s._change_type = 'delete' THEN
# MAGIC DELETE
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (Country, Product, SalesRate) VALUES (s.Country, s.Product, s.Sales/s.Stock)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM gold_table

# COMMAND ----------

