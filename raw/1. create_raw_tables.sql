-- Databricks notebook source
-- MAGIC %md
-- MAGIC Creating external tables from original files

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create circuits table (from csv)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT, 
circuitRef STRING, 
name STRING, 
location STRING, 
country STRING, 
lat DOUBLE, 
lng DOUBLE, 
alt INT, 
url STRING)
USING csv
OPTIONS(path "/mnt/kjmformula1dl/raw/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create races table (from csv)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date STRING,
time STRING,
url STRING)
USING CSV
OPTIONS(path "/mnt/kjmformula1dl/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create constructors table (from single line JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT, 
constructorRef STRING, 
name STRING, 
nationality STRING, 
url STRING)
USING JSON
OPTIONS(path "/mnt/kjmformula1dl/raw/constructors.json")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Create drivers table (from single line JSON w/ complex structure)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING JSON
OPTIONS(path "/mnt/kjmformula1dl/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create results table (from single line JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed STRING,
statusId INT
)
USING JSON
OPTIONS(path "/mnt/kjmformula1dl/raw/results.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create pitstops file (from multiline JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pitstops;
CREATE TABLE IF NOT EXISTS f1_raw.pitstops(
raceId INT,
driverId INT,
stop INT,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING JSON
OPTIONS(path "/mnt/kjmformula1dl/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create lap times table (from CSVs in folder)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT)
USING CSV
OPTIONS(path "/mnt/kjmformula1dl/raw/lap_times")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create qualifying table (from multiline JSONS in folder)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING JSON
OPTIONS(path "/mnt/kjmformula1dl/raw/qualifying", multiLine true)

-- COMMAND ----------

DESCRIBE EXTENDED f1_raw.qualifying;

-- COMMAND ----------


