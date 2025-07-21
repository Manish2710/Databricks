# Databricks notebook source
# MAGIC %md
# MAGIC **Sample Data**

# COMMAND ----------

structureData = [("James", 111, "HR"), ("Micheal", 222, "IT"), ("Robert", 333, "SALES"), ("Maria", 444, "IT"), ("Jen", 555, "HR")]

# COMMAND ----------

# MAGIC %md
# MAGIC **Define Structure Using Struct Type and Struct Field**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

structureSchema = StructType([StructField("Name", StringType(), True), StructField("ID", IntegerType(), True), StructField("Department", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC **Create DataFrame based on Structure Definition**

# COMMAND ----------

df = spark.createDataFrame(data = structureData, schema = structureSchema)
df.printSchema()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Nested Data**

# COMMAND ----------

structureData = [(("James", "Will", "Smith"), 111, "HR"), (("Micheal", "Rose", "Dan"), 222, "IT"), (("Robert", "Roy", "Williams"), 333, "SALES"), (("Maria", "Anne", "Jones"), 444, "IT"), (("Jen", "Mary", "Brown"), 555, "HR")]

# COMMAND ----------

# MAGIC %md
# MAGIC **Define Nested Structure**

# COMMAND ----------

structureSchema = StructType([StructField("Name", StructType([StructField("FirstName",StringType(), False),StructField("MiddleName", StringType(), True), StructField("LastName", StringType(), True)])) , StructField("ID", IntegerType(), True), StructField("Department", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC **Create DataFrame Using Nested Structure**

# COMMAND ----------

dfNested = spark.createDataFrame(data = structureData, schema = structureSchema)
dfNested.printSchema()
dfNested.display()

# COMMAND ----------

