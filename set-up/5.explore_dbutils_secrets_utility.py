# Databricks notebook source
# MAGIC %md
# MAGIC ####Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula1-secret')

# COMMAND ----------

client_id = dbutils.secrets.get('formula1-secret','formula1-secret-client-id')
client_secret = dbutils.secrets.get('formula1-secret','formula1-secret-client-secret')
tenant_id=  dbutils.secrets.get('formula1-secret','formula1-secret-tenant-id')

# COMMAND ----------


