# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest pit_stops file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the json using the spark dataframe reader
# MAGIC 1. using folder specified in configuration file
# MAGIC 1. specifying schema via StructType
# MAGIC 1. renaming the columns to match conventions
# MAGIC 1. adding data source column
# MAGIC 1. adding ingestion time column using add_ingestion_date method
# MAGIC ##### Write data to datalake as parquet file 
# MAGIC 1. using folder specified in configuration file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

# COMMAND ----------

# specifying schema                                   
pitstops_schema = StructType(fields= [StructField("raceId", IntegerType(), False), 
                                      StructField("driverId", IntegerType(), False),
                                      StructField("stop", IntegerType(), False), 
                                      StructField("lap", IntegerType(), True), 
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True), 
                                      StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

# reading json file using above schema
pitstops_df = spark.read.json(f"{raw_folder_path}/pit_stops.json", schema= pitstops_schema, multiLine= True)

# COMMAND ----------

# renaming columns + adding data source column
pitstops_renamed_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# adding ingestion date
pitstops_ingestion_df = add_ingestion_date(pitstops_renamed_df)

# COMMAND ----------

# write to a parquet file, partitioned by race_id
#pitstops_renamed_df.write.parquet(f"{processed_folder_path}/pitstops", mode="overwrite")

# Writing data to table in Data lake
pitstops_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

dbutils.notebook.exit("Success")
