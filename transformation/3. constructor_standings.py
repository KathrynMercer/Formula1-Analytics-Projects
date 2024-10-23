# Databricks notebook source
# MAGIC %run "../../Formula1-Analytics-Projects/ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Produce Team Standings Table

# COMMAND ----------

# MAGIC %md
# MAGIC Teams are ranked each season first by point total and then by wins
# MAGIC
# MAGIC Contents:
# MAGIC - race_year, team from race_results table
# MAGIC - sum of points per season
# MAGIC - sum of wins per season (determined by position)
# MAGIC - rank (generated)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

team_standings_df = race_results_df.groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

team_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = team_standings_df.withColumn("rank", rank().over(team_rank_spec))

# COMMAND ----------

#final_df.write.parquet(f"{presentation_folder_path}/constructor_standings", mode="overwrite")

# Writing data to table in Data lake
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
