# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys

# COMMAND ----------

spark.conf.set("fs.azure.account.key.databricksdlmg.dfs.core.windows.net","access_key_to_paste")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databricksdlmg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@databricksdlmg.dfs.core.windows.net/circuits.csv"))
