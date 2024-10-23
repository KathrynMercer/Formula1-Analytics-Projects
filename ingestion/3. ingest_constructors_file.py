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
# MAGIC 1. using folder specified in configuration file
# MAGIC 1. specifying schema via .DDL-formatted string
# MAGIC 1. dropping the url column
# MAGIC 1. renaming the columns to match conventions
# MAGIC 1. adding data source column
# MAGIC 1. adding ingestion time column using add_ingestion_date method
# MAGIC ##### Write data to datalake as parquet file
# MAGIC 1. using folder specified in configuration file

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# specifying schema using DDL formatted string
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# reading json file using above schema
constructors_df = spark.read.json(f"{raw_folder_path}/constructors.json", schema= constructors_schema)

# COMMAND ----------

# dropping url column
constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# renaming columns + adding data source column
constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                                 .withColumnRenamed("constructorRef", "constructor_ref") \
                                                 .withColumnRenamed("name", "constructor_name") \
                                                 .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# adding ingestion date
constructors_ingestion_date = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

# write to a parquet file
#constructors_ingestion_date.write.parquet(f"{processed_folder_path}/constructors", mode="overwrite")

# Writing data to table & parquet file in Data lake
constructors_ingestion_date.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
