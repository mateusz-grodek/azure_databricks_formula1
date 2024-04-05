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

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])



# COMMAND ----------

qualifying_df = spark.read \
                    .schema(qualifying_schema) \
                    .option("multiLine", True) \
                    .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

final_df = qualifying_with_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id") \
                                            .withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("raceId", "race_id") \
                                            .withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# qualifying_df_with_columns_df.write.mode("overwrite").parquet("/mnt/databricksdlmg/proccess/qualifying")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
