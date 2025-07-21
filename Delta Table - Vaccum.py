# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE scd2demo1 (
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
# MAGIC LOCATION '/FileStore/tables/scd2Demo1'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into scd2demo1 values(111, 'Unit1', 200, 500, 800, 400, 'Y', current_timestamp(), '9999-12-31');
# MAGIC insert into scd2demo1 values(222, 'Unit2', 900, Null, 700, 100, 'Y', current_timestamp(), '9999-12-31');
# MAGIC insert into scd2demo1 values(333, 'Unit3', 300, 900, 250, 650, 'Y', current_timestamp(), '9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into scd2demo1 values(666, 'Unit6', 200, 500, 800, 400, 'Y', current_timestamp(), '9999-12-31');
# MAGIC insert into scd2demo1 values(777, 'Unit7', 900, Null, 700, 100, 'Y', current_timestamp(), '9999-12-31');
# MAGIC insert into scd2demo1 values(888, 'Unit8', 300, 900, 250, 650, 'Y', current_timestamp(), '9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from scd2demo1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from scd2demo1 where pk1=777

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC update scd2demo1
# MAGIC set dim1 = 100 where pk1 = 666

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE scd2demo1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history scd2demo1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from scd2demo1

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC VACUUM scd2demo1 DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC VACUUM scd2Demo1 RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = False

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC VACUUM scd2demo1

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE scd2demo1
# MAGIC ZORDER  BY pk1

# COMMAND ----------

