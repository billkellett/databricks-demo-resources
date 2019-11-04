// Databricks notebook source
// MAGIC %md
// MAGIC ## PREP Notebook for demo of Delta Time Travel
// MAGIC 
// MAGIC Do a Run All on this PREP notebook to do all the "behind-the-scenes preparation" for the accompanying Delta Time Travel Demo notebook.
// MAGIC 
// MAGIC This PREP notebook takes roughly 10 minutes to run.
// MAGIC 
// MAGIC In this PREP notebook, we do the following:
// MAGIC 
// MAGIC - Download and ingest raw data (an initial data load plus 5 small files that represent update batches)
// MAGIC - Convert raw data to Parquet
// MAGIC - Convert raw data to Delta
// MAGIC - Perform 5 update MERGE operations on the Delta data
// MAGIC 
// MAGIC After this PREP notebook runs, we'll have 6 versions of the data.  We will now be ready to run the accompanying DEMO notebook that will actually be used in front of prospects.
// MAGIC 
// MAGIC #### NOTE:
// MAGIC This PREP notebook is idempotent. You can run it repeatedly without re-booting the Spark cluster.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Ingest raw data

// COMMAND ----------

// MAGIC %sh
// MAGIC # Find the working directory on the local file system
// MAGIC pwd

// COMMAND ----------

// MAGIC %sh
// MAGIC # Create a download directory on the local file system
// MAGIC mkdir demo-data

// COMMAND ----------

// MAGIC %sh
// MAGIC # download data into the local file system
// MAGIC curl -o /databricks/driver/demo-data/time_travel_demo_initial_set.csv 'https://raw.githubusercontent.com/billkellett/databricks-demo-time-travel-resources/master/data/time_travel_demo_initial_set.csv' -L 
// MAGIC curl -o /databricks/driver/demo-data/time_travel_demo_update_1.csv 'https://raw.githubusercontent.com/billkellett/databricks-demo-time-travel-resources/master/data/time_travel_demo_update_1.csv' -L 
// MAGIC curl -o /databricks/driver/demo-data/time_travel_demo_update_2.csv 'https://raw.githubusercontent.com/billkellett/databricks-demo-time-travel-resources/master/data/time_travel_demo_update_2.csv' -L 
// MAGIC curl -o /databricks/driver/demo-data/time_travel_demo_update_3.csv 'https://raw.githubusercontent.com/billkellett/databricks-demo-time-travel-resources/master/data/time_travel_demo_update_3.csv' -L 
// MAGIC curl -o /databricks/driver/demo-data/time_travel_demo_update_4.csv 'https://raw.githubusercontent.com/billkellett/databricks-demo-time-travel-resources/master/data/time_travel_demo_update_4.csv' -L 
// MAGIC curl -o /databricks/driver/demo-data/time_travel_demo_update_5.csv 'https://raw.githubusercontent.com/billkellett/databricks-demo-time-travel-resources/master/data/time_travel_demo_update_5.csv' -L 

// COMMAND ----------

// MAGIC %md
// MAGIC Create a data directory on dbfs

// COMMAND ----------

// MAGIC %fs
// MAGIC mkdirs /demo-time-travel/raw

// COMMAND ----------

// MAGIC %md
// MAGIC Copy data from the local file system to dbfs

// COMMAND ----------

// MAGIC %fs
// MAGIC cp file:/databricks/driver/demo-data/time_travel_demo_initial_set.csv dbfs:/demo-time-travel/raw/time_travel_demo_initial_set.csv

// COMMAND ----------

// MAGIC %fs
// MAGIC cp file:/databricks/driver/demo-data/time_travel_demo_update_1.csv dbfs:/demo-time-travel/raw/time_travel_demo_update_1.csv

// COMMAND ----------

// MAGIC %fs
// MAGIC cp file:/databricks/driver/demo-data/time_travel_demo_update_2.csv dbfs:/demo-time-travel/raw/time_travel_demo_update_2.csv

// COMMAND ----------

// MAGIC %fs
// MAGIC cp file:/databricks/driver/demo-data/time_travel_demo_update_3.csv dbfs:/demo-time-travel/raw/time_travel_demo_update_3.csv

// COMMAND ----------

// MAGIC %fs
// MAGIC cp file:/databricks/driver/demo-data/time_travel_demo_update_4.csv dbfs:/demo-time-travel/raw/time_travel_demo_update_4.csv

// COMMAND ----------

// MAGIC %fs
// MAGIC cp file:/databricks/driver/demo-data/time_travel_demo_update_5.csv dbfs:/demo-time-travel/raw/time_travel_demo_update_5.csv

// COMMAND ----------

// MAGIC %fs
// MAGIC ls demo-time-travel/raw

// COMMAND ----------

// A reference to our csv
val csvFile = "demo-time-travel/raw/time_travel_demo_initial_set.csv"

val rawDF = spark.read             // The DataFrameReader
  .option("header", "true")        // Use first line of all files as header
  .option("delimiter", ",")        // Comma is default, but specifying here for clarity
  .option("inferSchema", "true")   // Tell Spark to figure out the schema
  .csv(csvFile)                    // Creates a DataFrame from CSV after reading in the file

val csvUpdate1 = "demo-time-travel/raw/time_travel_demo_update_1.csv"

val rawUpdate1DF = spark.read             // The DataFrameReader
  .option("header", "true")        // Use first line of all files as header
  .option("delimiter", ",")        // Comma is default, but specifying here for clarity
  .option("inferSchema", "true")   // Tell Spark to figure out the schema
  .csv(csvUpdate1)                    // Creates a DataFrame from CSV after reading in the file

val csvUpdate2 = "demo-time-travel/raw/time_travel_demo_update_2.csv"

val rawUpdate2DF = spark.read             // The DataFrameReader
  .option("header", "true")        // Use first line of all files as header
  .option("delimiter", ",")        // Comma is default, but specifying here for clarity
  .option("inferSchema", "true")   // Tell Spark to figure out the schema
  .csv(csvUpdate2)                    // Creates a DataFrame from CSV after reading in the file

val csvUpdate3 = "demo-time-travel/raw/time_travel_demo_update_3.csv"

val rawUpdate3DF = spark.read             // The DataFrameReader
  .option("header", "true")        // Use first line of all files as header
  .option("delimiter", ",")        // Comma is default, but specifying here for clarity
  .option("inferSchema", "true")   // Tell Spark to figure out the schema
  .csv(csvUpdate3)                    // Creates a DataFrame from CSV after reading in the file

val csvUpdate4 = "demo-time-travel/raw/time_travel_demo_update_4.csv"

val rawUpdate4DF = spark.read             // The DataFrameReader
  .option("header", "true")        // Use first line of all files as header
  .option("delimiter", ",")        // Comma is default, but specifying here for clarity
  .option("inferSchema", "true")   // Tell Spark to figure out the schema
  .csv(csvUpdate4)                    // Creates a DataFrame from CSV after reading in the file

val csvUpdate5 = "demo-time-travel/raw/time_travel_demo_update_5.csv"

val rawUpdate5DF = spark.read             // The DataFrameReader
  .option("header", "true")        // Use first line of all files as header
  .option("delimiter", ",")        // Comma is default, but specifying here for clarity
  .option("inferSchema", "true")   // Tell Spark to figure out the schema
  .csv(csvUpdate5)                    // Creates a DataFrame from CSV after reading in the file

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Convert raw data into Parquet format
// MAGIC 
// MAGIC This is useful if I want to show things you **can't** do in Parquet.

// COMMAND ----------

// MAGIC %fs
// MAGIC mkdirs demo-time-travel/parquet

// COMMAND ----------

val fileName = "demo-time-travel/parquet/time_travel_demo_initial_set_parquet"

rawDF.write                        // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(fileName)               // Write DataFrame to Parquet files

val update1_fileName = "demo-time-travel/parquet/time_travel_demo_update_1_parquet"

rawUpdate1DF.write                        // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(update1_fileName)               // Write DataFrame to Parquet files

val update2_fileName = "demo-time-travel/parquet/time_travel_demo_update_2_parquet"

rawUpdate2DF.write                        // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(update2_fileName)               // Write DataFrame to Parquet files

val update3_fileName = "demo-time-travel/parquet/time_travel_demo_update_3_parquet"

rawUpdate3DF.write                        // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(update3_fileName)               // Write DataFrame to Parquet files

val update4_fileName = "demo-time-travel/parquet/time_travel_demo_update_4_parquet"

rawUpdate4DF.write                        // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(update4_fileName)               // Write DataFrame to Parquet files

val update5_fileName = "demo-time-travel/parquet/time_travel_demo_update_5_parquet"

rawUpdate5DF.write                        // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(update5_fileName)               // Write DataFrame to Parquet files

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3: Convert raw data into Delta format

// COMMAND ----------

// Create table
rawDF.createOrReplaceTempView("raw_data")

// COMMAND ----------

// MAGIC %md
// MAGIC Remove folder if it exists

// COMMAND ----------

dbutils.fs.rm("demo-time-travel/delta/time_travel_demo_full_delta/", true)  // recurse=True

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- We created a temp view of our "legacy" data earlier, so we can use SQL to create and populate the new Delta Lake table.
// MAGIC DROP TABLE IF EXISTS cust_life_spend_delta;
// MAGIC 
// MAGIC CREATE TABLE cust_life_spend_delta
// MAGIC USING delta
// MAGIC LOCATION '/demo-time-travel/delta/time_travel_demo_full_delta'
// MAGIC AS SELECT * FROM raw_data;
// MAGIC 
// MAGIC -- View Delta Lake table
// MAGIC SELECT * FROM cust_life_spend_delta;

// COMMAND ----------

// Create table
rawUpdate1DF.createOrReplaceTempView("raw_update1")
rawUpdate2DF.createOrReplaceTempView("raw_update2")
rawUpdate3DF.createOrReplaceTempView("raw_update3")
rawUpdate4DF.createOrReplaceTempView("raw_update4")
rawUpdate5DF.createOrReplaceTempView("raw_update5")

// COMMAND ----------

// MAGIC %md
// MAGIC We don't need the original update files any more.

// COMMAND ----------

dbutils.fs.rm("demo-time-travel/delta/time_travel_demo_update_1_delta/", true)  // recurse=True
dbutils.fs.rm("demo-time-travel/delta/time_travel_demo_update_2_delta/", true)  // recurse=True
dbutils.fs.rm("demo-time-travel/delta/time_travel_demo_update_3_delta/", true)  // recurse=True
dbutils.fs.rm("demo-time-travel/delta/time_travel_demo_update_4_delta/", true)  // recurse=True
dbutils.fs.rm("demo-time-travel/delta/time_travel_demo_update_5_delta/", true)  // recurse=True

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Let's create tables for all of our updates.
// MAGIC -- We created a temp view of our "legacy" data earlier, so we can use SQL to create and populate the new Delta Lake table.
// MAGIC DROP TABLE IF EXISTS cust_life_spend_delta_update_1;
// MAGIC 
// MAGIC CREATE TABLE cust_life_spend_delta_update_1
// MAGIC USING delta
// MAGIC LOCATION '/demo-time-travel/delta/time_travel_demo_update_1_delta'
// MAGIC AS SELECT * FROM raw_update1;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS cust_life_spend_delta_update_2;
// MAGIC 
// MAGIC CREATE TABLE cust_life_spend_delta_update_2
// MAGIC USING delta
// MAGIC LOCATION '/demo-time-travel/delta/time_travel_demo_update_2_delta'
// MAGIC AS SELECT * FROM raw_update2;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS cust_life_spend_delta_update_3;
// MAGIC 
// MAGIC CREATE TABLE cust_life_spend_delta_update_3
// MAGIC USING delta
// MAGIC LOCATION '/demo-time-travel/delta/time_travel_demo_update_3_delta'
// MAGIC AS SELECT * FROM raw_update3;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS cust_life_spend_delta_update_4;
// MAGIC 
// MAGIC CREATE TABLE cust_life_spend_delta_update_4
// MAGIC USING delta
// MAGIC LOCATION '/demo-time-travel/delta/time_travel_demo_update_4_delta'
// MAGIC AS SELECT * FROM raw_update4;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS cust_life_spend_delta_update_5;
// MAGIC 
// MAGIC CREATE TABLE cust_life_spend_delta_update_5
// MAGIC USING delta
// MAGIC LOCATION '/demo-time-travel/delta/time_travel_demo_update_5_delta'
// MAGIC AS SELECT * FROM raw_update5;

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 4: Perform update batches in order to create a table with history we can use for time travel

// COMMAND ----------

// Now, at 1-minute intervals, we'll do each of our updata batches
Thread.sleep(60000L)
spark.sql("""MERGE INTO cust_life_spend_delta as target
USING cust_life_spend_delta_update_1 as updates
on target.id = updates.id
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT *""")

Thread.sleep(60000L)
spark.sql("""MERGE INTO cust_life_spend_delta as target
USING cust_life_spend_delta_update_2 as updates
on target.id = updates.id
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT *""")

Thread.sleep(60000L)
spark.sql("""MERGE INTO cust_life_spend_delta as target
USING cust_life_spend_delta_update_3 as updates
on target.id = updates.id
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT *""")

Thread.sleep(60000L)
spark.sql("""MERGE INTO cust_life_spend_delta as target
USING cust_life_spend_delta_update_4 as updates
on target.id = updates.id
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT *""")

Thread.sleep(60000L)
spark.sql("""MERGE INTO cust_life_spend_delta as target
USING cust_life_spend_delta_update_5 as updates
on target.id = updates.id
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT *""")

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE DETAIL cust_life_spend_delta

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Now we have 6 versions of the table, created at roughly 1-minute intervals 
// MAGIC DESCRIBE HISTORY cust_life_spend_delta

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * from cust_life_spend_delta
// MAGIC ORDER BY id ASC
