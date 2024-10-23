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
# MAGIC ### Ingest qualifying files from folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the json using the spark dataframe reader
# MAGIC
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
qualifying_schema = StructType(fields= [StructField("constructorId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), False),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True), 
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                      StructField("qualifyId", IntegerType(), True), 
                                      StructField("raceId", IntegerType(), True) 
                                    ])

# COMMAND ----------

# reading json file using above schema
qualifying_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/qualifying", schema= qualifying_schema, multiLine= True)

# COMMAND ----------

# renaming columns + adding data source column 
qualifying_renamed_df = qualifying_df.withColumnRenamed("qualifyId", "qualifying_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("constructorId", "constructor_id") \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# add ingestion date
qualifying_ingestion_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# write to a parquet file
#qualifying_ingestion_df.write.parquet(f"{processed_folder_path}/qualifying", mode="overwrite")

# Writing data to table in Data lake
#qualifying_ingestion_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

#incremental load write to Data lake
overwrite_partition(qualifying_ingestion_df, "f1_processed", "qualifying", "race_id") 

# COMMAND ----------

dbutils.notebook.exit("Success")
