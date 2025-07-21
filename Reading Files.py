# Databricks notebook source
# MAGIC %md
# MAGIC **Reading the CSV File**

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/baby_names')

#dbutils.fs.rm("/FileStore/tables/baby_names", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the Single CSV file**

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").load("/FileStore/tables/baby_names/baby_names_2007_2009.csv")

display(df)

print(df.count())

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").load("/FileStore/tables/baby_names/baby_names_2010_2012.csv")

display(df)

print(df.count())

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").load("/FileStore/tables/baby_names/baby_names_2013_2014.csv")

display(df)

print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the Multiple CSV file**

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").load(["/FileStore/tables/baby_names/baby_names_2007_2009.csv", "/FileStore/tables/baby_names/baby_names_2010_2012.csv"])

display(df)

print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Read all files under a Folder**

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").load("/FileStore/tables/baby_names/")

display(df)

print(df.count())

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Own Schema**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_defined = StructType([StructField('Year', IntegerType(), True),
                             StructField('Name', StringType(), True),
                             StructField('Country', StringType(), True),
                             StructField('Sex', StringType(), True),
                             StructField('Count', IntegerType(), True)])

# COMMAND ----------

df = spark.read.format("csv").schema(schema_defined).option("header", True).option("sep", ",").load("/FileStore/tables/baby_names/")

display(df)

print(df.count())

# COMMAND ----------

schema_alternate = 'Year INTEGER, Name STRING, Country STRING, Sex STRING, Count INTEGER'

# COMMAND ----------

df = spark.read.format("csv").schema(schema_alternate).option("header", True).option("sep", ",").load("/FileStore/tables/baby_names/")

display(df)

print(df.count())

# COMMAND ----------

df.printSchema()

# COMMAND ----------

