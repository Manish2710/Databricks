# Databricks notebook source
# MAGIC %md
# MAGIC **Review CSV file data**

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head /FileStore/tables/baby_names_id.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ****

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

fullDf = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").load("/FileStore/tables/baby_names_id.csv")

fullDf.display()

print(fullDf.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Skip First 4 Records while Reading CSV File and Display Data and Record Count**

# COMMAND ----------

skipDf = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").option("skipRows", 4).load("/FileStore/tables/baby_names_id.csv")

skipDf.display()

print(skipDf.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **How to skip records based on specific range in middle of csv file, say for example rows 11 to 20**

# COMMAND ----------

# MAGIC %md
# MAGIC **Step-2: Create data by reading data only from starting row of specific Range**

# COMMAND ----------

skipStartDf = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").option("skipRows", 10).load("/FileStore/tables/baby_names_id.csv")

skipStartDf.display()

print(skipStartDf.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Step-3: Create data by reading data only from Ending row of specific Range**

# COMMAND ----------

skipEndDf = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").option("skipRows", 20).load("/FileStore/tables/baby_names_id.csv")

skipEndDf.display()

print(skipEndDf.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Step-3: Find Delta between fullDF and skipStartDf**

# COMMAND ----------

deltaDf = fullDf.subtract(skipStartDf)
display(deltaDf)

# COMMAND ----------

# MAGIC %md
# MAGIC **Combine deltaDf and skipEndDf**

# COMMAND ----------

finalDf = deltaDf.union(skipEndDf)
display(finalDf.orderBy("Id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **UDF**

# COMMAND ----------

def skipRowsRangeCSV(filepath, startPos, endPos):
  fullDf = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").load(filepath)

  skipStartDf = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").option("skipRows", startPos).load(filepath)

  skipEndDf = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").option("skipRows", endPos).load(filepath)

  deltaDf = fullDf.subtract(skipStartDf)
  finalDf = deltaDf.union(skipEndDf)

  return finalDf

# COMMAND ----------

resultDf = skipRowsRangeCSV(filepath='/FileStore/tables/baby_names_id.csv', startPos=10, endPos=20)

display(resultDf.orderBy("Id"))