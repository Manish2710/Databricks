# Databricks notebook source
# MAGIC %md
# MAGIC **Create First Dataframe**

# COMMAND ----------

empData1 = [(111,"Stephen","King",2000),
            (222,"Philipp","Larkin",8000),
            (333,"John","Smith",6000)
           ]

empSchema1 = ["Id","FirstName","LastName","salary"]

df1 = spark.createDataFrame(data=empData1, schema = empSchema1)

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Second Dataframe with Same Schema as First Dataframe**

# COMMAND ----------

empData2 = [(444,"Thomas","Frank",4000),
            (555,"Stephen","Fleming",3000),
            (666,"William","Pending",7000)
           ]

empSchema2 = ["Id","FirstName","LastName","salary"]

df2 = spark.createDataFrame(data=empData2, schema = empSchema2)

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Third Dataframe with Different Schema from First Dataframe**

# COMMAND ----------

empData3 = [(777,"David",4000),
            (888,"Mike",3000),
            (999,"Winsten",7000)
           ]

empSchema3 = ["Id","Name","salary"]

df3 = spark.createDataFrame(data=empData3, schema = empSchema3)

display(df3)


# COMMAND ----------

# MAGIC %md
# MAGIC **Compare Schema of First and Second DataFrame**

# COMMAND ----------

if df1.schema == df2.schema:
    print("Schema Matches")
else:
    print("Schema Does Not Match")

# COMMAND ----------

# MAGIC %md
# MAGIC **Compare Schema of First and Third Dataframe**

# COMMAND ----------

if df1.schema == df3.schema:
    print("Schema Matches")
else:
    print("Schema Does Not Match")

# COMMAND ----------

# MAGIC %md
# MAGIC **List of Columns Missing in Third Dataframe**

# COMMAND ----------

print(list(set(df2.columns) - set(df3.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC **List of Columns Missing in Second Dataframe**

# COMMAND ----------

print(list(set(df3.columns) - set(df2.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC **Collect All Possible Columns in a List**

# COMMAND ----------

allColumns = df1.columns + df3.columns
uniqueColumns = list(set(allColumns))
print(uniqueColumns)

# COMMAND ----------

# MAGIC %md
# MAGIC **Add Missing Columns**

# COMMAND ----------

from pyspark.sql.functions import lit
for col in uniqueColumns:
  if col not in df1.columns:
    df1 = df1.withColumn(col, lit(None))
  if col not in df3.columns:
    df3 = df3.withColumn(col, lit(None))

display(df1)
display(df3)

# COMMAND ----------

def addMissingColumns(df1, df2):
    allColumns = df1.columns+df2.columns
    uniqueColumns = list(set(allColumns))
    for col in uniqueColumns:
        if col not in df1.columns:
            df1 = df1.withColumn(col, lit(None))
        if col not in df3.columns:
            df2 = df2.withColumn(col, lit(None))
    return df1, df2

# COMMAND ----------

df1, df3 = addMissingColumns(df1, df3)
display(df1)
display(df3)

# COMMAND ----------

