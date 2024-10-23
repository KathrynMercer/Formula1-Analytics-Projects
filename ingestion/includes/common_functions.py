# Databricks notebook source
# add ingestion date column
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

# organize columns with partition by column at the end
# print error message if partition column not found
def partition_at_end(input_df, partition_column):
    columns = input_df.schema.names
    if partition_column in columns:
        columns.remove(partition_column)
        columns.append(partition_column)
        output_df = input_df.select(columns)
        return output_df
    else:
        print("Partition column not found")

# COMMAND ----------

# incremental load write to data lake

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = partition_at_end(input_df, partition_column)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")
