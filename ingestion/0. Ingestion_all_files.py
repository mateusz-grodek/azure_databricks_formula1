# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook runner
# MAGIC #### This notebook run all ingestion file one by one with 2 parameters:
# MAGIC 1. data_source
# MAGIC 2. file_date

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# Databricks notebook source
v_result = dbutils.notebook.run("1. Ingestion circuits file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2. Ingestion races file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3. Ingest constructor json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4. Ingest drivers json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5. Ingest results json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6. Ingest pitstops json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7. Ingestlabs csv files", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8. Ingest qualifying  json files", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------


