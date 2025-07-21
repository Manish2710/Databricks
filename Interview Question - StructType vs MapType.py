# Databricks notebook source
# MAGIC %md
# MAGIC **Define Struct Type Data**

# COMMAND ----------

structureData = [(("James", "Will", "Smith"), 111, "HR"), (("Micheal", None, "Dan"), 222, "IT"), (("Robert", "Roy", "Williams"), 333, "SALES"), (("Maria", "Anne", "Jones"), 444, "IT"), (("Jen", "Mary", "Brown"), 555, "HR")]

# COMMAND ----------

# MAGIC %md
# MAGIC **Define Struct Type Schema**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

structureSchema = StructType([StructField("Name", StructType([StructField("FirstName",StringType(), False),StructField("MiddleName", StringType(), True), StructField("LastName", StringType(), True)])) , StructField("ID", IntegerType(), True), StructField("Department", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC **Create DataFrame with Nested Columns of Struct Type**

# COMMAND ----------

dfNested = spark.createDataFrame(data = structureData, schema = structureSchema)
dfNested.printSchema()
dfNested.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Map Type data and schema**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

data = [(111, "Mike", {'TV':'LG', 'Refridgerator':'Samsung', 'Oven':'Phillips', 'AC':'Voltas'}),
        (222, "David", {'AC':'Samsung', 'Washing Machine':'LG'}),
        (333, "Williams", {'TV':'Croma'}),
        (444, "Williams", None)]

schema = StructType([
  StructField('ID', IntegerType(), True),
  StructField('Name', StringType(), True),
  StructField('Utilities', MapType(StringType(), StringType()), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Dataframe using Map Type**

# COMMAND ----------

dfNested = spark.createDataFrame(data = data, schema = schema)
dfNested.printSchema()
dfNested.display()

# COMMAND ----------

