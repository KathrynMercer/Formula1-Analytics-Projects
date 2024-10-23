# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the csv using the spark dataframe reader
# MAGIC 1. using folder specified in configuration file
# MAGIC 1. using header from csv
# MAGIC 1. specifying schema manually
# MAGIC 1. renaming the columns to match conventions
# MAGIC 1. combining date + time columns into new race_timestamp column
# MAGIC 1. adding data source column
# MAGIC 1. adding ingestion date column using add_ingestion_date method
# MAGIC 1. dropping the url, time, date columns
# MAGIC ##### Write data to datalake as parquet file
# MAGIC 1. using folder specified in configuration file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, lit, concat, to_timestamp

# COMMAND ----------

# inferSchema = true adds an extra job, so I'm assigning the schema manually instead
# This decision also means if the data is incorrectly formated, an error will be thrown before my data is "contaminated"

races_schema = StructType(fields= [StructField("raceId", IntegerType(), False), 
                                      StructField("year", IntegerType(), True),
                                      StructField("round", IntegerType(), True), 
                                      StructField("circuitId", IntegerType(), True), 
                                      StructField("name", StringType(), True), 
                                      StructField("date", StringType(), True), 
                                      StructField("time", StringType(), True), 
                                      StructField("url", StringType(), True)])


# COMMAND ----------

races_df = spark.read.csv(f'{raw_folder_path}/races.csv', header= True, schema = races_schema)

# COMMAND ----------

# Renaming columns to match naming conventions & write out abbreviations for readability
# and adding data source column
races_renamed_df = races_df.withColumnRenamed("raceId", "race_id") \
                            .withColumnRenamed("year", "race_year") \
                            .withColumnRenamed("circuitId", "circuit_id") \
                            .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# forming the race_timestamp from the date & time columns
races_timestamp_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# Selecting only the required columns from the raw data (AKA no time, date, url column)
races_selected_df = races_timestamp_df.select("race_id", "race_year", "round", "circuit_id", "name", "race_timestamp")

# COMMAND ----------

# Adding ingestion date
races_final_df = add_ingestion_date(races_selected_df)

# COMMAND ----------

# Writing data to Parquet file in Data lake
#races_final_df.write.parquet(f"{processed_folder_path}/races", mode= 'overwrite', partitionBy=["race_year"])

# Writing data to table in Data lake
races_final_df.write.mode("overwrite").format("parquet").partitionBy(["race_year"]).saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
