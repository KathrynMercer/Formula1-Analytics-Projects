# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21") #default value is first folder date
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the csv using the spark dataframe reader
# MAGIC 1. using folder specified in configuration file
# MAGIC 1. using file date (for incremental load)
# MAGIC 1. using header from csv
# MAGIC 1. specifying schema manually
# MAGIC 1. dropping the url column
# MAGIC 1. renaming the columns to match conventions
# MAGIC 1. adding data source column
# MAGIC 1. adding ingestion time column using add_ingestion_date method
# MAGIC ##### Write data to datalake as parquet file
# MAGIC 1. using folder specified in configuration file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import lit

# COMMAND ----------

# inferSchema = true adds an extra job, so I'm assigning the schema manually instead
# This decision also means if the data is incorrectly formated, an error will be thrown before my data is "contaminated"

circuits_schema = StructType(fields= [StructField("circuitId", IntegerType(), False), 
                                      StructField("circuitRef", StringType(), True), 
                                      StructField("name", StringType(), True), 
                                      StructField("location", StringType(), True), 
                                      StructField("country", StringType(), True), 
                                      StructField("lat", DoubleType(), True), 
                                      StructField("long", DoubleType(), True), 
                                      StructField("alt", IntegerType(), True), 
                                      StructField("url", StringType(), True)])


# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", 
                             header= True, 
                             schema= circuits_schema)

# COMMAND ----------

# Selecting only the required columns from the raw data (AKA no url column)
circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", 
                                          "country", "lat", "long", "alt")

# COMMAND ----------

# Renaming columns to match naming conventions & write out abbreviations for readability
# and adding data source column
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitID", "circuit_id") \
                                        .withColumnRenamed("circuitRef", "circuit_ref") \
                                        .withColumnRenamed("lat", "latitude") \
                                        .withColumnRenamed("long", "longitude") \
                                        .withColumnRenamed("alt", "altitude") \
                                        .withColumn("data_source", lit(v_data_source))\
                                        .withColumn("file date", lit(v_file_date))

# COMMAND ----------

# Adding ingestion date column
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# Writing data to Parquet file in Data lake
#circuits_final_df.write.parquet(f"{processed_folder_path}/circuits", mode= 'overwrite')

# Writing data to Parquet file & table in Data lake
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


