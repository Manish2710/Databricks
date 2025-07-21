# Databricks notebook source
# MAGIC %md
# MAGIC **Sample DataFrame**

# COMMAND ----------

employee_data = [(111, "Stephen", "King", 2000),
                 (222, "Philipp", "Larkin", 8000),
                 (333, "John", "Smith", 6000)]

employee_schema = ["Id", "FirstName", "LastName", "salary"]

df = spark.createDataFrame(data=employee_data, schema=employee_schema)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Add New Column with Multiple withColumn**

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

dfWithColumn = df.withColumn("Name", concat(col("FirstName"), lit(" "), col("LastName"))) \
                 .withColumn("BonusPercent", lit(10)) \
                 .withColumn("TotalSalary", col("salary") * col("BonusPercent")) \
                 .withColumn("DateCreated", current_timestamp())

display(dfWithColumn)

# COMMAND ----------

# MAGIC %md
# MAGIC **Add New Column with Multiple DataFrames**

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

dfWithCol = df.withColumn("Name", concat(col("FirstName"), lit(" "), col("LastName")))
dfWithCol = dfWithCol.withColumn("BonusPercent", lit(10))
dfWithCol = dfWithCol.withColumn("TotalSalary", col("Salary") * col("BonusPercent"))
dfWithCol = dfWithCol.withColumn("DateCreated", current_timestamp())

display(dfWithCol)

# COMMAND ----------

# MAGIC %md
# MAGIC **Add new columns through UDF**

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp
def addAuditCOls(df):
  df = df.withColumn("dateCreated", current_timestamp()).withColumn("CreatedByUser", lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())).withColumn("createdByPipeline", lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()))

  return df

# COMMAND ----------

dfAudit = addAuditCOls(df)
display(dfAudit)

# COMMAND ----------

# MAGIC %md
# MAGIC **SELECT vs WITHCOLUMN**

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

dfSelect = df.select("*", concat(col("FirstName"), lit(" "), col("LastName")).alias("Name"), lit(10).alias("BonusPercent"), col("salary") * lit(10).alias("TotalSalary"), current_timestamp().alias("DateCreated"))

display(dfSelect)


dfWithColumn = df.withColumn("Name", concat(col("FirstName"), lit(" "), col("LastName"))) \
                 .withColumn("BonusPercent", lit(10)) \
                 .withColumn("TotalSalary", col("salary") * col("BonusPercent")) \
                 .withColumn("DateCreated", current_timestamp())

display(dfWithColumn)

# COMMAND ----------

