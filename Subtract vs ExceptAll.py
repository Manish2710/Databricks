# Databricks notebook source
# MAGIC %md
# MAGIC ### **subtract vs exceptAll**

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Sample Dataframe**

# COMMAND ----------

data1 = [("Product A", 100),
         ("Product B", 50),
         ("Product C", 200),
         ("Product C", 200),
         ("Product D", 150)]

data2 = [("Product A", 100),
         ("Product B", 30),
         ("Product C", 200),
         ("Product D", 80)]

sourceDf = spark.createDataFrame(data1, ["Product", "Quantity"])
targetDf = spark.createDataFrame(data2, ["Product", "Quantity"])

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify Data**

# COMMAND ----------

display(sourceDf)
display(targetDf)

# COMMAND ----------

# MAGIC %md
# MAGIC **Subtract - Not Preserving the duplicates**

# COMMAND ----------

resultDf = sourceDf.subtract(targetDf)
resultDf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **exceptAll - Preserves Duplicates**

# COMMAND ----------

resultDf = sourceDf.exceptAll(targetDf)
resultDf.display()

# COMMAND ----------

