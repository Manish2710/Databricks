# Databricks notebook source
data = [("ABC pvt ltd", "Q1", 2000),
        ("XYZ pvt ltd", "Q1", 5000),
        ("KLM pvt ltd", "Q1", 2000)
        ]

column = ["Company", "Quarter", "Revenue"]
df = spark.createDataFrame(data, column)
display(df)

# COMMAND ----------

df.write.saveAsTable("default.fact_revenue")

# COMMAND ----------

display(spark.table("default.fact_revenue"))

# COMMAND ----------

display(spark.sql("select * from default.fact_revenue"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from default.fact_revenue

# COMMAND ----------

data = [("RST pvt ltd", "Q4", 7000)]

column = ["Company", "Quarter", "Revenue"]

df1 = spark.createDataFrame(data, column)
display(df1)

# COMMAND ----------

df1.write.insertInto("default.fact_revenue", overwrite = False)

# COMMAND ----------

display(spark.table("default.fact_revenue"))

# COMMAND ----------

data = [("QRT pvt ltd", "Q3", 3000)]

column = ["Company", "Quarter", "Revenue"]

df2 = spark.createDataFrame(data, column)
display(df2)

# COMMAND ----------

df2.write.insertInto("default.fact_revenue", overwrite = True)

# COMMAND ----------

data = [("ABC pvt ltd", "Q3", 4000)]

column = ["Company", "Quarter", "Revenue"]

df3 = spark.createDataFrame(data, column)
display(df3)

# COMMAND ----------

df3.createOrReplaceTempView("v_insert_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into default.fact_revenue
# MAGIC select * from v_insert_data

# COMMAND ----------

display(spark.table("default.fact_revenue"))

# COMMAND ----------

