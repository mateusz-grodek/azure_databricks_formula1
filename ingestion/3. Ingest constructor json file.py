# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name  STRING, nationality STRING, url STRING"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# constructor_df = spark.read \
#                 .schema(constructors_schema) \
#                 .json("/mnt/databricksdlmg/raw/constructors.json")
constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")


# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col,lit

# COMMAND ----------

constructor_renamed_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_renamed_df)

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet("/mnt/databricksdlmg/proccess/constructors")
constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

spark.read.parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
