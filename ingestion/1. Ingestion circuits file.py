# Databricks notebook source
# MAGIC %md 
# MAGIC #### STEP 1. Read csv

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

#Add datasource dynamic varaible using widget. We can change this value running the notebook.
dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

v_data_source

# COMMAND ----------

#Add datasource_date dynamic varaible using widget. We can change this value running the notebook.
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

#display avaiable mount folder
display(dbutils.fs.mounts())

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType,DoubleType,StructField,StructType

# COMMAND ----------

#prepare tabel schema
circuits_schema = StructType(fields = [StructField("circuitId",IntegerType(),False),
                                       StructField("circuitRef",StringType(),True),
                                       StructField("name",StringType(),True),
                                       StructField("location",StringType(),True),
                                       StructField("country",StringType(),True),
                                       StructField("lat",DoubleType(),True),
                                       StructField("lng",DoubleType(),True),
                                       StructField("alt",IntegerType(),True),
                                       StructField("url",StringType(),True),

                                       ])

# COMMAND ----------

#run configuration notebook with mounted file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# circuits_df = spark.read \
#     .option("header", True) \
#     .option("inferSchema",True) \
#     .csv("dbfs:/mnt/databricksdlmg/raw/circuits.csv")

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"dbfs:{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP 2. Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")
#or
#from pyspark.sql.functions import col
#circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
#or
#circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df["lat"], circuits_df["lng"],circuits_df.alt)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3. Renama columns

# COMMAND ----------

#import lit function. lit allows you to fill the column with the same value.
from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuits_id") \
            .withColumnRenamed("circuitRef","circuit_ref") \
            .withColumnRenamed("lat","latitute") \
            .withColumnRenamed("lng","longitude") \
            .withColumnRenamed("alt","altitude") \
            .withColumn("data_source", lit(v_data_source)) \
            .withColumn("file_date", lit(v_file_date))
                

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

###run notebook common_function to import function

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp, lit
# circuits_final_df = add_ingestion_date(circuits_renamed_df).withColumn("env",lit("Production"))
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

#Overwrite data in datalake
# circuits_final_df.write.mode("overwrite").parquet("/mnt/databricksdlmg/processed/circuits")

#Overwrite data in datalake making table in db catalog
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/circuits"))

# COMMAND ----------

spark.read.parquet("/mnt/databricksdlmg/processed/circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
