-- Databricks notebook source
-- MAGIC %md
-- MAGIC Creating table view of race results
-- MAGIC - assigning custom points values based on wins 
-- MAGIC - save as table for future reference

-- COMMAND ----------

USE f1_processed

-- COMMAND ----------

-- calculated points = 11-position since points assigned per win has not been consistent across all years
-- this guarantees that the points awarded are consistent accross years (ex. first place always recieves 10 points)
CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year,
      constructors.constructor_name as team_name,
      drivers.name as driver_name,
      results.position,
      results.points, 
      11 - results.position as calculated_points
FROM f1_processed.results
JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
JOIN f1_processed.races ON (results.race_id = races.race_id)
WHERE results.position <= 10;

-- COMMAND ----------


