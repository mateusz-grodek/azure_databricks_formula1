# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


