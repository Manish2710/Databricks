# Databricks notebook source
data = [("ABC","Q1",2000),
        ("ABC","Q2",3000),
        ("ABC","Q3",6000),
        ("ABC","Q4",1000),
        ("XYZ","Q1",5000),
        ("XYZ","Q2",4000),
        ("XYZ","Q3",2000),
        ("XYZ","Q4",3000),
        ("KLM","Q1",2000),
        ("KLM","Q2",1000),
        ("KLM","Q3",3000),
        ("KLM","Q4",5000)]

column = ["Company", "Quarter", "Revenue"]
df = spark.createDataFrame(data = data, schema = column)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pivot a DataFrame**

# COMMAND ----------

pivot_df = df.groupBy("Company").pivot("Quarter").sum("Revenue")

display(df)

display(pivot_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Unpivot a DataFrame**

# COMMAND ----------

df1 = pivot_df.selectExpr("Company", "stack(4, 'Q1',Q1,'Q2',Q2,'Q3',Q3,'Q4',Q4) as (Quarter, Revenue)")

display(df1)

# COMMAND ----------

