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
# MAGIC ### Ingest results file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the json using the spark dataframe reader
# MAGIC 1. using folder specified in configuration file
# MAGIC 1. specifying schema via StructType
# MAGIC 1. renaming the columns to match conventions
# MAGIC 1. adding data source column
# MAGIC 1. adding ingestion time column using add_ingestion_date method
# MAGIC 1. dropping the statusId column
# MAGIC ##### Write data to datalake as parquet file (partitioned by race_id)
# MAGIC 1. using folder specified in configuration file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import lit

# COMMAND ----------

# specifying schema                                   
results_schema = StructType(fields= [StructField("resultId", IntegerType(), False), 
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True), 
                                      StructField("constructorId", IntegerType(), True), 
                                      StructField("number", IntegerType(), True),
                                      StructField("grid", IntegerType(), True), 
                                      StructField("position", IntegerType(), True),
                                      StructField("positionText", StringType(), True),
                                      StructField("positionOrder", IntegerType(), True),
                                      StructField("points", FloatType(), True),
                                      StructField("laps", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("fastestLap", IntegerType(), True),
                                      StructField("rank", IntegerType(), True),
                                      StructField("fastestLapTime", StringType(), True),
                                      StructField("fastestLapSpeed", StringType(), True),
                                      StructField("statusId", IntegerType(), True)])

# COMMAND ----------

# reading json file using above schema
results_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json", schema= results_schema)

# COMMAND ----------

# renaming columns + adding data source column
results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("constructorId", "constructor_id") \
                                .withColumnRenamed("positionText", "position_text") \
                                .withColumnRenamed("positionOrder", "position_order") \
                                .withColumnRenamed("fastestLap", "fastest_lap") \
                                .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# adding ingestion date
results_ingestion_date_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# dropping statusId column
results_dropped_df = results_ingestion_date_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC First implementation of incremental load using
# MAGIC drop  if race_id already exists in table (less efficient w/ partitions)

# COMMAND ----------

# # determine what race_ids are contained within the dataframe,
# # drop them if they are already in the processed results, so they can be added below 
# # without introducting duplicates
# for race_id_list in results_dropped_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql("ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# # write to a parquet file, partitioned by race_id
# #results_dropped_df.write.parquet(f"{processed_folder_path}/results", mode="overwrite", partitionBy= "race_id")

# # Writing data to table in Data lake (using append for incremental load via race_id)
# results_dropped_df.write.mode("append").format("parquet").partitionBy(["race_id"]).saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC New incremental load implementation w/ dynamic overwrite

# COMMAND ----------

overwrite_partition(results_dropped_df, "f1_processed", "results", "race_id") 

# COMMAND ----------

dbutils.notebook.exit("Success")
