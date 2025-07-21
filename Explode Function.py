# Databricks notebook source
# MAGIC %md
# MAGIC **Create dataframe with array column**

# COMMAND ----------

array_appliances = [("Raja", ["TV", "Refridgerator", "Oven", "AC"]), ("Raghav", ["AC", "Washing Machine", None]), ("Ramesh", ["Refridgerator", "TV", None]), ("Rajesh", None)]

df = spark.createDataFrame(array_appliances, schema = ["Name", "Appliances"])

df.printSchema()

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create dataframe with map column**

# COMMAND ----------

map_brand = [
  ("Raja", {"TV":"LG", "Refridgerator":"Samsung", "Oven":"Phillips", "AC":"Voltas"}),
  ("Raghav", {"AC":"Samsung", "Washing Machine":"LG"}),
  ("Ram", {"Grinder":"Preethi", "TV":""}),
  ("Ramesh", {"Refridgerator":"LG", "TV":"Croma"}),
  ("Rajesh", None)
]

df_brand = spark.createDataFrame(map_brand, schema=["name", "brand"])

df_brand.printSchema()

display(df_brand)

# COMMAND ----------

# MAGIC %md
# MAGIC **Explode Array Field**

# COMMAND ----------

from pyspark.sql.functions import explode

df2 = df.select(df.Name, explode(df.Appliances))

df.printSchema()
display(df)

df2.printSchema()
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Explode Map Field**

# COMMAND ----------

from pyspark.sql.functions import explode

df3 = df_brand.select(df_brand.name, explode(df_brand.brand))

df.printSchema()
display(df)

df2.printSchema()
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Explode outer to consider NULL Values**

# COMMAND ----------

from pyspark.sql.functions import explode_outer

display(df.select(df.Name, explode_outer(df.Appliances)))

display(df_brand.select(df_brand.name, explode_outer(df_brand.brand)))

# COMMAND ----------

# MAGIC %md
# MAGIC **Positional Explode**

# COMMAND ----------

from pyspark.sql.functions import posexplode

display(df.select(df.Name, posexplode(df.Appliances)))

display(df_brand.select(df_brand.name, posexplode(df_brand.brand)))

# COMMAND ----------

# MAGIC %md
# MAGIC **Positional Explode with Outer**

# COMMAND ----------

from pyspark.sql.functions import posexplode_outer

display(df.select(df.Name, posexplode_outer(df.Appliances)))

display(df_brand.select(df_brand.name, posexplode_outer(df_brand.brand)))

# COMMAND ----------

