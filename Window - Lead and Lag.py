# Databricks notebook source
# MAGIC %md
# MAGIC **Window Function Lead and Lag**

# COMMAND ----------

simpleData = (("James", "Sales", 3000), 
("Michael", "Sales", 4600), 
("Robert", "Sales", 4100), 
("James", "Sales", 3000), 
("Saif", "Sales", 4100), 
("Maria", "Finance", 3000), 
("Scott", "Finance", 3300), 
("Jen", "Finance", 3900), 
("Jeff", "Marketing", 3000), 
("Kumar", "Marketing", 2000) 
)
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("department").orderBy("salary")

# COMMAND ----------

# MAGIC %md
# MAGIC **Lag Window Function**

# COMMAND ----------

from pyspark.sql.functions import lag

df.withColumn("lag", lag("salary", 1).over(windowSpec)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Lead Window Function**

# COMMAND ----------

from pyspark.sql.functions import lead

df.withColumn("lead", lead("salary", 1).over(windowSpec)).show()

# COMMAND ----------

