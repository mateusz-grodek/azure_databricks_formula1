# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal

# COMMAND ----------

client_id = dbutils.secrets.get('formula1-secret','formula1-secret-client-id')
client_secret = dbutils.secrets.get('formula1-secret','formula1-secret-client-secret')
tenant_id=  dbutils.secrets.get('formula1-secret','formula1-secret-tenant-id')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databricksdlmg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databricksdlmg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databricksdlmg.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databricksdlmg.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databricksdlmg.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databricksdlmg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@databricksdlmg.dfs.core.windows.net/circuits.csv"))
