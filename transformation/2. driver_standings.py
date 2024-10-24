# Databricks notebook source
# MAGIC %run "../../Formula1-Analytics-Projects/ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Produce Driver Standings Table

# COMMAND ----------

# MAGIC %md
# MAGIC Drivers are ranked each season first by point total and then by wins
# MAGIC
# MAGIC Contents:
# MAGIC - race_year, driver_name, driver_number, driver_nationality, team from race_results table
# MAGIC - sum of points per season
# MAGIC - sum of wins per season (determined by position)
# MAGIC - rank (generated)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#final_df.write.parquet(f"{presentation_folder_path}/driver_standings", mode="overwrite")

# Writing data to table in Data lake
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------


