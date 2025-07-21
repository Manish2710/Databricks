# Databricks notebook source
# MAGIC %md
# MAGIC **Cryptography Library Installation**

# COMMAND ----------

pip install cryptography

# COMMAND ----------

# MAGIC %md
# MAGIC **Generate Encryption/Decryption Key**

# COMMAND ----------

from cryptography.fernet import Fernet

key = Fernet.generate_key()
f = Fernet(key)

# COMMAND ----------

# MAGIC %md
# MAGIC **Encrypt Sample Data**

# COMMAND ----------

PIIData = b"testmail@gmail.com"
TestData = f.encrypt(PIIData)
print(TestData)

# COMMAND ----------

# MAGIC %md
# MAGIC **Decrypt Sample Data**

# COMMAND ----------

print(f.decrypt(TestData))

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Sample Delta Table**

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark).tableName("dimEmployee").addColumn("empID", "INT").addColumn("empName", "STRING").addColumn("SSN", "STRING").execute()

# COMMAND ----------

# MAGIC %md
# MAGIC **Insert PII data**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into dimEmployee values(100, 'Mike', '2345671');
# MAGIC insert into dimEmployee values(200, 'David', '5632178');
# MAGIC insert into dimEmployee values(300, 'Peter', '1456782');

# COMMAND ----------

# MAGIC %md
# MAGIC **View Data**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dimEmployee;

# COMMAND ----------

# MAGIC %md
# MAGIC **Define UDF to Encrypt Data**

# COMMAND ----------

def encrypt_data(data, KEY):
    f = Fernet(KEY)
    dataB = bytes(data, 'utf-8')
    encrypt_data = f.encrypt(dataB)
    encrypt_data = str(encrypt_data.decode('ascii'))
    return encrypt_data

# COMMAND ----------

# MAGIC %md
# MAGIC **Define UDF to Decrypt Data**

# COMMAND ----------

def decrypt_data(encrypted_data, KEY):
  f = Fernet(KEY)
  decrypt_data = f.decrypt(encrypted_data.encode()).decode()
  return decrypt_data

# COMMAND ----------

# MAGIC %md
# MAGIC **Register UDFs**

# COMMAND ----------

from pyspark.sql.functions import udf, lit, md5
from pyspark.sql.types import StringType

encryption = udf(encrypt_data, StringType())
decryption = udf(decrypt_data, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC **Encrypt the data**

# COMMAND ----------

df = spark.table("dimEmployee")
encrypteddf = df.withColumn("ssn_encrypted", encryption("SSN", lit(key)))
display(encrypteddf)

# COMMAND ----------

# MAGIC %md
# MAGIC **Decrypt the data**

# COMMAND ----------

decrypteddf = encrypteddf.withColumn("ssn_decrypted", decryption("ssn_encrypted", lit(key)))
display(decrypteddf)

# COMMAND ----------

