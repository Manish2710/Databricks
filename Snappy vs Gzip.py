# Databricks notebook source
dbutils.fs.mkdirs("/FileStore/tables/read_files")

# COMMAND ----------

csvfile = "/FileStore/tables/read_files/baby_names.csv"

csvDF = (spark.read.option('sep', ",").option("inferSchema", True).csv(csvfile))

display(csvDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **Snappy**

# COMMAND ----------

csvDF.write.format("parquet").option("compression", "snappy").save("/FileStore/tables/bigdata_training/write_files/snappy_parquets")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/bigdata_training/write_files/snappy_parquets/

# COMMAND ----------

csvDF.write.format("parquet").option("compression", "gzip").save("/FileStore/tables/bigdata_training/write_files/gzip_parquets")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/bigdata_training/write_files/gzip_parquets/

# COMMAND ----------

