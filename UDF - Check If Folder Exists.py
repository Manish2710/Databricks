# Databricks notebook source
dbutils.fs.unmount("/mnt/adls_test") ...

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs:                                         
  mount_point = "/mnt/adls_test",
  extra_configs = {("fs.azure.account.key.adlsrajade.blob.core.windows.net": "MBnAFm5wvvaJLPJ4pYwsI6Tycq4zOogW2DW0w13MA0CZ+cTrY9swcLVp79+96MTfnkUAt sCtl t9zx+ASte90fNw=="})

# COMMAND ----------

# MAGIC %fs ls /mnt/adls_test/sales/

# COMMAND ----------

def folderExists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as err:
        if 'java.io.FileNotFoundException' in str(err):
            return False

# COMMAND ----------

path = "/mnt/adls_test/sales/2018/"

if not folderExists(path):
    print("Folder is not present")
    #dbutils.fs.mkdirs(path)
else:
    print("Folder is already present")

# COMMAND ----------

path = "/mnt/adls_test/sales/2030/"

if not folderExists(path):
    print("Folder is not present")
    #dbutils.fs.mkdirs(path)
else:
    print("Folder is already present")

# COMMAND ----------

