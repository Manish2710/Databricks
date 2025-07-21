# Databricks notebook source
Transaction = [(100,'Cosmetic', 150), (200, 'Apparel', 250), (300, 'Shirt', 400), (400, 'Trouser', 500), (500, 'Socks', 20),
               (100, 'Belt', 70), (200, 'Cosmetic', 250), (300, 'Shoe', 400), (400, 'Socks', 25), (500, 'Shorts', 100)]

transactionDF = spark.createDataFrame(data = Transaction, schema = ['Store_id', 'Item', 'Amount'])
transactionDF.show()

# COMMAND ----------

Store =  [(100, 'Store_London'), (200, 'Store_Paris'), (300, 'Store_Frankfurt'), (400, 'Store_Stockholm'), (500, 'Store_Oslo')]

storeDF = spark.createDataFrame(data = Store, schema=['Store_id', 'Store_name'])

storeDF.show()

# COMMAND ----------

from pyspark.sql.functions import broadcast

joinDF = transactionDF.join(broadcast(storeDF), transactionDF.Store_id == storeDF.Store_id)

joinDF.show()

# COMMAND ----------

joinDF.explain(True)

# COMMAND ----------

