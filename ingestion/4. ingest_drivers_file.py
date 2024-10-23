# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest constructors file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the json using the spark dataframe reader
# MAGIC 1. specifying schema via StructType
# MAGIC 1. combinine forename + surname into one name column
# MAGIC 1. renaming the columns to match conventions
# MAGIC 1. adding data source column
# MAGIC 1. adding ingestion time column using add_ingestion_date method
# MAGIC 1. dropping the url column
# MAGIC ##### Write data to datalake as parquet file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, lit, concat, to_timestamp

# COMMAND ----------

# name schema exists as inner json 
name_schema = StructType(fields= [StructField("forename", StringType(), True),
                                  StructField("surname", StringType(), True)])

# drivers schema references above name schema                                   
drivers_schema = StructType(fields= [StructField("driverId", IntegerType(), False), 
                                      StructField("driverRef", StringType(), True),
                                      StructField("number", IntegerType(), True), 
                                      StructField("code", StringType(), True), 
                                      StructField("name", name_schema, True),
                                      StructField("dob", DateType(), True), 
                                      StructField("nationality", StringType(), True), 
                                      StructField("url", StringType(), True)])

# COMMAND ----------

# reading json file using above schema
drivers_df = spark.read.json(f"{raw_folder_path}/drivers.json", schema= drivers_schema)

# COMMAND ----------

# renaming columns + concatenating fore- & sur- name columns into 1 full name column
# and adding data source column
drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("driverRef", "driver_ref") \
                                .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# adding ingestion date
drivers_ingestion_date_df = add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

# dropping url column
drivers_dropped_df = drivers_ingestion_date_df.drop("url")

# COMMAND ----------

# write to a parquet file
#drivers_dropped_df.write.parquet(f"{processed_folder_path}/drivers", mode="overwrite")

# Writing data to table & parquet file in Data lake
drivers_dropped_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
