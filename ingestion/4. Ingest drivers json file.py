# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType( fields= [StructField("forename",StringType(),True),
                                   StructField("surname",StringType(),True)])

# COMMAND ----------

drivers_schema = StructType( fields= [StructField("driverId", IntegerType(), False),
                                      StructField("driverRef", StringType(), False),
                                      StructField("number", IntegerType(), False),
                                      StructField("code", StringType(), False),
                                      StructField("name", name_schema ),
                                      StructField("dob", DateType(), False),
                                      StructField("nationality", StringType(), False),
                                      StructField("url", StringType(), False)
                                      ])


# COMMAND ----------

# drivers_df = spark.read.schema(drivers_schema).json("/mnt/databricksdlmg/raw/drivers.json")
drivers_df = spark.read \
                .schema(drivers_schema) \
                .json(f"{raw_folder_path}/{v_file_date}/drivers.json")


# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
