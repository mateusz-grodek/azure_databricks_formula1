# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.key.databricksdlmg.dfs.core.windows.net","access_key_to_paste")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databricksdlmg.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@databricksdlmg.dfs.core.windows.net/circuits.csv"))
