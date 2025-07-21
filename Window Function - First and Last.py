# Databricks notebook source
# MAGIC %md
# MAGIC **Create Sample Dataframe**

# COMMAND ----------

from pyspark.sql.functions import *

data = [
    ("C1", "2023-06-01", 100.0),
    ("C1", "2023-06-02", 150.0),
    ("C1", "2023-06-03", 200.0),
    ("C2", "2023-06-01", 50.0),
    ("C2", "2023-06-02", 75.0),
    ("C2", "2023-06-03", 100.0),
]

df = spark.createDataFrame(data, ["customer_id", "transaction_date", "amount"])

df = df.withColumn("transaction_date", to_date(col("transaction_date")))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Window Function**

# COMMAND ----------

from pyspark.sql.window import Window

windowSpec = Window.partitionBy("customer_id")

result_df = df.withColumn("first_transaction_date", first("transaction_date").over(windowSpec)).withColumn("last_transaction_date", last("transaction_date").over(windowSpec))

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Finding First and Last Transaction Date for each customer**

# COMMAND ----------

result_df = df.withColumn("first_transaction_date", first("transaction_date").over(windowSpec)).withColumn("last_transaction_date", last("transaction_date").over(windowSpec)).drop("transaction_date", "amount").distinct()

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Using Spark SQL**

# COMMAND ----------

# Register DataFrame as a temporary view
df.createOrReplaceTempView("transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL Window Function**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT customer_id,
# MAGIC FIRST(transaction_date) OVER (PARTITION BY customer_id ) AS first_transaction_date,
# MAGIC LAST(transaction_date) OVER (PARTITION BY customer_id ) AS last_transaction_date
# MAGIC FROM transactions
# MAGIC ORDER BY customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT customer_id,
# MAGIC FIRST(transaction_date) OVER (PARTITION BY customer_id ORDER BY customer_id) AS first_transaction_date,
# MAGIC LAST(transaction_date) OVER (PARTITION BY customer_id ORDER BY customer_id) AS last_transaction_date
# MAGIC FROM transactions

# COMMAND ----------

