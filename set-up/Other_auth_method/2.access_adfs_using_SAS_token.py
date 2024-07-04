# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using sas token

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databricksdlmg.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databricksdlmg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databricksdlmg.dfs.core.windows.net","SAS_token_to_paste")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databricksdlmg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@databricksdlmg.dfs.core.windows.net/circuits.csv"))
