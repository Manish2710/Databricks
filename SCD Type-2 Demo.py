# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE scd2demo (
# MAGIC   pk1 INT,
# MAGIC   pk2 STRING,
# MAGIC   dim1 INT,
# MAGIC   dim2 INT,
# MAGIC   dim3 INT,
# MAGIC   dim4 INT,
# MAGIC   active_status STRING,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP)
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/tables/scd2Demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into scd2demo values(111, 'Unit1', 200, 500, 800, 400, 'Y', current_timestamp(), '9999-12-31');
# MAGIC insert into scd2demo values(222, 'Unit2', 900, Null, 700, 100, 'Y', current_timestamp(), '9999-12-31');
# MAGIC insert into scd2demo values(333, 'Unit3', 300, 900, 250, 650, 'Y', current_timestamp(), '9999-12-31');

# COMMAND ----------

from delta import *

targetTable = DeltaTable.forPath(spark, "/FileStore/tables/scd2Demo")
targetDf = targetTable.toDF()
display(targetDf)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([StructField("pk1", StringType(), True),
                     StructField("pk2", StringType(), True),
                     StructField("dim1", IntegerType(), True),
                     StructField("dim2", IntegerType(), True),
                     StructField("dim3", IntegerType(), True),
                     StructField("dim4", IntegerType(), True)])

# COMMAND ----------

data = [(111, 'Unit1', 200, 500, 800, 400),
        (222, 'Unit2', 800, 1300, 800, 500),
        (444, 'Unit4', 100, None, 700, 300)]

sourceDf = spark.createDataFrame(data, schema)
display(sourceDf)

# COMMAND ----------

joinDf = sourceDf.join(targetDf, (sourceDf.pk1 == targetDf.pk1) & (sourceDf.pk2 == targetDf.pk2) & (targetDf.active_status == 'Y'), "leftouter").select(sourceDf["*"], targetDf.pk1.alias("target_pk1"), targetDf.pk2.alias("target_pk2"), targetDf.dim1.alias("target_dim1"), targetDf.dim2.alias("target_dim2"), targetDf.dim3.alias("target_dim3"), targetDf.dim4.alias("target_dim4"))

display(joinDf)

# COMMAND ----------

filterDf = joinDf.filter(xxhash64(joinDf.dim1, joinDf.dim2, joinDf.dim3, joinDf.dim4) != xxhash64(joinDf.target_dim1, joinDf.target_dim2, joinDf.target_dim3, joinDf.target_dim4))

display(filterDf)

# COMMAND ----------

mergeDf = filterDf.withColumn("MergeKey", concat(filterDf.pk1, filterDf.pk2))

display(mergeDf)

# COMMAND ----------

dummyDf = filterDf.filter("target_pk1 is not null").withColumn("MergeKey", lit(None))

display(dummyDf)

# COMMAND ----------

scdDf = mergeDf.union(dummyDf)

display(scdDf)

# COMMAND ----------

targetTable.alias("target").merge(
    source = scdDf.alias("source"), condition = "concat(target.pk1, target_pk2) = source.MergeKey and target.active_status = 'Y' ").whenMatchedUpdate(set = {
        "active_status": "'N'",
        "end_date": "current_date"
    }).whenNotMatchedInsert(values =
                            {
                                "pk1": "source.pk1",
                                "pk2": "source.pk2",
                                "dim1": "source.dim1",
                                "dim2": "source.dim2",
                                "dim3": "source.dim3",
                                "dim4": "source.dim4",
                                "active_status": "'Y'",
                                "start_date": "current_date",
                                "end_date": """to_date('9999-12-31','yyyy-MM-dd')"""
                            }).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from scd2demo

# COMMAND ----------

