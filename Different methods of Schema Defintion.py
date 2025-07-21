# Databricks notebook source
# MAGIC %md
# MAGIC **First Method**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data1 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
        ]

schema1 = StructType([ \
    StructField("firstname", StringType(), False), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
])

df = spark.createDataFrame(data=data1, schema=schema1)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Nested Schema Definition**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

structureData = [(("James", "", "Smith"), "36636", "M", 3000),
         (("Michael", "Rose", ""), "40288", "M", 4000),
         (("Robert", "", "Williams"), "42114", "M", 4000),
         (("Maria", "Anne", "Jones"), "39192", "F", 4000),
         (("Jen", "Mary", "Brown"), "", "F", -1)
        ]

structureSchema = StructType([ \
    StructField("name",StructType([StructField("firstname", StringType(), False), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True)])), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
])

df = spark.createDataFrame(data=structureData, schema=structureSchema)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Array Type and Map Type**

# COMMAND ----------

from pyspark.sql.types import *

arrayStructureSchema = StructType([
    StructField("name", StructType([
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True)
    ])),
    StructField("hobbies", ArrayType(StringType(), True)),
    StructField("properties", MapType(StringType(), StringType(), True))
])

arraystructureData = [
    (("James", "", "Smith"), ["music", "reading", "chess"], {"salary": "3000", "dept": "HR"}),
    (("Michael", "Rose", ""), ["music", "playing", "chess"], {"salary": "3000", "dept": "HR"}),
    (("Robert", "", "Williams"), ["music", "chess"], {"salary": "3000", "dept": "HR"}),
    (("Maria", "Anne", "Jones"), ["music", "reading"], {"salary": "3000", "dept": "HR"}),
    (("Jen", "Mary", "Brown"), ["music"], {"salary": "3000", "dept": "HR"})
]

arrayDF = spark.createDataFrame(data=arraystructureData, schema=arrayStructureSchema)

display(arrayDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Definition for Reading Big Data Files**

# COMMAND ----------

mySchema = StructType([StructField("manufacturer", StringType(), False), StructField("country", StringType(), True)])

df = spark.read.format("csv").option("header", True).schema(mySchema).load("/FileStore/tables/manufacturers.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Second Method of Creating Schema**

# COMMAND ----------

inlineSchema = "manufacturerName STRING, country STRING"

df = spark.read.format("csv").option("header", True).schema(inlineSchema).load("/FileStore/tables/manufacturers.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Third Methid of Creating Schema**

# COMMAND ----------

data = [("Mike", "M", 50000, 2), ("David", "F", 45000, 3), ("Thomas", "M", 47000, 2), ("William", "M", 40000, 4), ("Steve", "F", 35000, 5)]

schema = ["Name of employee", "Gender", "Salary", "Years Of Experience"]

df = spark.createDataFrame(data, schema)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Schema Along with Dataframe**

# COMMAND ----------

df = spark.createDataFrame([("Mazda RX4", 21, 4, 2), ("Hornet 4 Drive", 22, 3, 2), ("Merc 2400", 25, 4, 2), ("Lotus Europa", 31, 5, 2), ("Ferrari Dino", 20, 5, 6), ("Volvo 142E", 22, 4, 2)], ["Car Name", "mgp", "gear", "carb"])

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print(df.schema)

# COMMAND ----------

print(df.schema.json())

# COMMAND ----------

