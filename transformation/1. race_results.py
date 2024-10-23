# Databricks notebook source
# MAGIC %run "../../Formula1-Analytics-Projects/ingestion/includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21") #default value is first folder date
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Race Results Table

# COMMAND ----------

# MAGIC %md
# MAGIC Contents:
# MAGIC - race_year, race_name, race_date from races table
# MAGIC - circuit_location from circuits table
# MAGIC - driver_name, driver_number, driver_nationality from drivers table
# MAGIC - team from constructors table
# MAGIC - grid, fastest lap, race time, position, points from results table
# MAGIC - created_date (generated timestamp on table creation)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, desc

# COMMAND ----------

# reading all tables necessary (filtered by year = 2020)
#full load data
races_df = spark.read.parquet(f"{processed_folder_path}/races")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
teams_df = spark.read.parquet(f"{processed_folder_path}/constructors")

# incremental load data
results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# Joining races & circuits tables, selecting relevant columns, and providing aliases to match requirements
races_joined_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
.select(races_df.race_year, 
        races_df.name.alias("race_name"), 
        races_df.race_timestamp.alias("race_date"),
        circuits_df.location.alias("circuit_location"),
        races_df.race_id) # selected bc it's the primary key in results table

# COMMAND ----------

# Joining results/drivers/teams tables with above racer/circuit table, selecting relevant columns, and providing aliases to match requirements

races_joined_results_df = results_df.join(races_joined_circuits_df, results_df.race_id == races_joined_circuits_df.race_id)\
        .join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
        .join(teams_df, results_df.constructor_id == teams_df.constructor_id)\
.select(races_joined_circuits_df["*"],
        drivers_df.name.alias("driver_name"),
        drivers_df.number.alias("driver_number"),
        drivers_df.nationality.alias("driver_nationality"),
        teams_df.constructor_name.alias("team"),
        results_df.grid,
        results_df.fastest_lap_time,
        results_df.time.alias("race_time"),
        results_df.position,
        results_df.points)

# COMMAND ----------

# Dropping primary keys that aren't necessary to display
table_no_primaries = races_joined_results_df.drop("race_id")

# COMMAND ----------

# Adding created_date
final_table = table_no_primaries.withColumn("created_date", current_timestamp())

# COMMAND ----------

#final_table.write.parquet(f"{presentation_folder_path}/race_results", mode='overwrite')

# Writing data to table in Data lake
final_table.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
