# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest lap_times.csv files

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the csv files using the spark dataframe reader
# MAGIC 1. using folder specified in configuration file
# MAGIC 1. specifying schema manually
# MAGIC 1. renaming the columns to match conventions
# MAGIC 1. adding data source column
# MAGIC 1. adding ingestion time column using add_ingestion_date method
# MAGIC ##### Write data to datalake as parquet file
# MAGIC 1. using folder specified in configuration file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, lit, concat, to_timestamp

# COMMAND ----------

# set schema

lap_times_schema = StructType(fields= [StructField("raceId", IntegerType(), False), 
                                      StructField("driverId", IntegerType(), False),
                                      StructField("lap", IntegerType(), False), 
                                      StructField("position", IntegerType(), True), 
                                      StructField("time", StringType(), True),  
                                      StructField("milliseconds", IntegerType(), True)])


# COMMAND ----------

lap_times_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/lap_times', schema = lap_times_schema)

# COMMAND ----------

# Renaming columns to match naming conventions & write out abbreviations for readability
# and adding data source column
lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                            .withColumnRenamed("driverId", "driver_id") \
                            .withColumn("data_source", lit(v_data_source)) \
                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# add ingestion date
lap_times_ingestion_date_df = add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

# Writing data to Parquet file in Data lake
#lap_times_ingestion_date_df.write.parquet(f"{processed_folder_path}/lap_times", mode= 'overwrite')

# Writing data to table in Data lake
#lap_times_ingestion_date_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

#incremental load write to Data lake
overwrite_partition(lap_times_ingestion_date_df, "f1_processed", "lap_times", "race_id") 

# COMMAND ----------

dbutils.notebook.exit("Success")
