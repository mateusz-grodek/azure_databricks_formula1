# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

results_schema = StructType( fields= [StructField("resultId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("constructorId", IntegerType(), False),
                                      StructField("number",IntegerType(),False ),
                                      StructField("grid", IntegerType(), False),
                                      StructField("position", IntegerType(), False),
                                      StructField("positionText", StringType(), False),
                                      StructField("positionOrder", IntegerType(), False),
                                      StructField("points", IntegerType(), False),
                                      StructField("labs", IntegerType(), False),
                                      StructField("time", StringType(), False),
                                      StructField("milliseconds", IntegerType(), False),
                                      StructField("fastestLab", IntegerType(), False),
                                      StructField("rank", IntegerType(), False),
                                      StructField("fastestLabTime", StringType(), False),
                                      StructField("fastestLabSpeed", FloatType(), False),
                                      StructField("statusId", StringType(), False)
                                      ])


# COMMAND ----------

# results_df = spark.read.schema(results_schema).json("/mnt/databricksdlmg/raw/results.json")

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLab", "fastest_lap") \
                                    .withColumnRenamed("fastestLabTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLabSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(results_with_columns_df)

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

#1 method to input data with full ingestion
# results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/databricksdlmg/proccess/results")
# results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

#2 method to input data with incremental load. Check if partition exist. If exist drop partition ID. Then append data by partition id
# for race_id_list in results_final_df.select("race_id").distinct().collect():
#   if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")


#3 method input data using partitionOverwriteMode "dynamic". The fastest way.  overwrite_partition is self made function from common function notebook
overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results

# COMMAND ----------

dbutils.notebook.exit("Success")
