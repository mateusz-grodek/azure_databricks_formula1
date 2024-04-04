# Databricks notebook source
# MAGIC %md
# MAGIC ####Explore dbfs root

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

dbutils.secrets.list('formula1-secret')

# COMMAND ----------

client_id = dbutils.secrets.get('formula1-secret','formula1-secret-client-id')
client_secret = dbutils.secrets.get('formula1-secret','formula1-secret-client-secret')
tenant_id=  dbutils.secrets.get('formula1-secret','formula1-secret-tenant-id')

# COMMAND ----------


