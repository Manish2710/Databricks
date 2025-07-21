# Databricks notebook source
dbutils.fs.mkdirs('/FileStore/tables/unit_testing')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/unit_testing/

# COMMAND ----------

# MAGIC %md
# MAGIC **Library Installation**

# COMMAND ----------

# Install this library in Maven Coordinates in the cluster
# com.crealytics:spark-excel_2.12:0.13.5

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Single Sheet of Excel**

# COMMAND ----------

df = spark.read.format("com.crealytics.spark.excel").option("inferSchema", True).option("header", True).option("dataAddress", "sheet2!").load("/FileStore/tables/unit_testing/excel_source_data.xlsx")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Define Excel Location and Sheet Names**

# COMMAND ----------

sheets = ['sheet1', 'sheet2', 'sheet1']
path = "/FileStore/tables/unit_testing/excel_source_data.xlsx"

# COMMAND ----------

# MAGIC %md
# MAGIC **UDF to Create dataframe from Multiple Sheets and Perform Union**

# COMMAND ----------

def createExcelDataFrame(path, sheets):
  firstSheet = sheets[0]
  df = spark.read.format("com.crealytics.spark.excel").option("inferSchema", True).option("header", True).option("dataAddress", f"{firstSheet}!").load(path)
  schema = df.schema

  for sheet in sheets[1:]:
    sheetDf = spark.read.format("com.crealytics.spark.excel").option("inferSchema", True).option("header", True).option("dataAddress", f"{sheet}!").load(path)
    df = df.union(sheetDf)
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC **Call UDF and Display Output Dataframe**

# COMMAND ----------

outDf = createExcelDataFrame(path, sheets)
display(outDf)

# COMMAND ----------

