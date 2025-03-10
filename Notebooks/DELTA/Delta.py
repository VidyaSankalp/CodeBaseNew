# Databricks notebook source
# MAGIC %md
# MAGIC https://docs.delta.io/latest/index.html#

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse.test.test_table(
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   age INT,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   salary FLOAT
# MAGIC )
# MAGIC LOCATION 's3://prudhvi-healthcare-01272025/test_delta_table/'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY lakehouse.test.test_table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (1, 'John', 30, 'New York', 'NY', 100000.0)

# COMMAND ----------

display(spark.read.parquet("s3://prudhvi-healthcare-01272025/test_delta_table/part-00000-1b84fc21-82ae-4bdf-a804-7bdc2cebb4d4-c000.snappy.parquet"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY lakehouse.test.test_table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (2, 'Prudhvi', 32, 'Rajahmundry', 'RJY', 10000.0)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse.test.test_table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (2, 'Prudhvi', 32, 'Rajahmundry', 'RJY', 10000.0);
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (2, 'Prudhvi', 32, 'Rajahmundry', 'RJY', 10000.0);
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (2, 'Prudhvi', 32, 'Rajahmundry', 'RJY', 10000.0);
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (2, 'Prudhvi', 32, 'Rajahmundry', 'RJY', 10000.0);
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (2, 'Prudhvi', 32, 'Rajahmundry', 'RJY', 10000.0);
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (2, 'Prudhvi', 32, 'Rajahmundry', 'RJY', 10000.0);
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (2, 'Prudhvi', 32, 'Rajahmundry', 'RJY', 10000.0);
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (2, 'Prudhvi', 32, 'Rajahmundry', 'RJY', 10000.0);
# MAGIC INSERT INTO lakehouse.test.test_table VALUES (2, 'Prudhvi', 32, 'Rajahmundry', 'RJY', 10000.0);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lakehouse.test.test_table VERSION AS OF 22

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lakehouse.test.test_table

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE lakehouse.test.test_table SET age = 33 WHERE id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM lakehouse.test.test_table WHERE id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lakehouse_dev.default.test_table1

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakehouse_dev.default.test_table1 SET TBLPROPERTIES ("spark.databricks.delta.retentionDurationCheck.enabled"="false");

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM lakehouse_dev.default.test_table1

# COMMAND ----------

# MAGIC %sql
# MAGIC create table  lakehouse_dev.default.new_test_table as select * from lakehouse_dev.default.test_table1

# COMMAND ----------

# MAGIC %run ../common_utils

# COMMAND ----------

source_sinks = [
    {
        "source":{
            "path": "s3://prudhvi-08052024-test/dataset/parquet/allergies",
            "type": "file",
            "format": "parquet"
        },
        "sink":{
            "path": "s3://prudhvi-08052024-test/delta/allergies",
            "mode": "overwrite",
            "database": "lakehouse_dev.health_care",
            "table": "delta_allergies",
            "format": "delta"
        }
    },
    {
        "source":{
            "path": "s3://prudhvi-08052024-test/dataset/parquet/claims_transcations",
            "type": "file",
            "format": "parquet"
        },
        "sink":{
            "path": "s3://prudhvi-08052024-test/delta/claims_transcations",
            "mode": "overwrite",
            "database": "lakehouse_dev.health_care",
            "table": "delta_claims_transcations",
            "format": "delta"
        }
    },
    {
        "source":{
            "path": "s3://prudhvi-08052024-test/dataset/parquet/claims",
            "type": "file",
            "format": "parquet"
        },
        "sink":{
            "path": "s3://prudhvi-08052024-test/delta/claims",
            "mode": "overwrite",
            "database": "lakehouse_dev.health_care",
            "table": "delta_claims",
            "format": "delta"
        }
    },
    {
        "source":{
            "path": "s3://prudhvi-08052024-test/dataset/parquet/paitents",
            "type": "file",
            "format": "parquet"
        },
        "sink":{
            "path": "s3://prudhvi-08052024-test/delta/paitents",
            "mode": "overwrite",
            "database": "lakehouse_dev.health_care",
            "table": "delta_paitents",
            "format": "delta"
        }
    },
    {
        "source":{
            "path": "s3://prudhvi-08052024-test/dataset/parquet/payers",
            "type": "file",
            "format": "parquet"
        },
        "sink":{
            "path": "s3://prudhvi-08052024-test/delta/payers",
            "mode": "overwrite",
            "database": "lakehouse_dev.health_care",
            "table": "delta_payers",
            "format": "delta"
        }
    }
]

# COMMAND ----------

for source_sink in source_sinks:
    process_source_sink(source_sink)

# COMMAND ----------

# MAGIC %md
# MAGIC https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html
# MAGIC https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")
#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1G")
# spark.conf.set("spark.sql.shuffle.partitions", "200")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   * 
# MAGIC FROM 
# MAGIC   lakehouse.test.paitents dp
# MAGIC INNER JOIN 
# MAGIC   lakehouse.test.claims_transcations dct
# MAGIC ON dp.id = dct.PATIENTID

# COMMAND ----------

files = dbutils.fs.ls("s3://prudhvi-test-destination-02272025/dataset/parquet/paitents")
total_size = sum(f.size for f in files)
size_in_mb = total_size / (1024 * 1024)
print(f"Total size: {size_in_mb:.2f} MB")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL lakehouse.test.paitents;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   /*+ BROADCAST(dp) */
# MAGIC   * 
# MAGIC FROM 
# MAGIC   lakehouse.test.paitents dp
# MAGIC INNER JOIN 
# MAGIC   lakehouse.test.claims_transcations dct
# MAGIC ON dp.id = dct.PATIENTID;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   * 
# MAGIC FROM 
# MAGIC   lakehouse_dev.health_care.delta_paitents dp
# MAGIC INNER JOIN 
# MAGIC   lakehouse_dev.health_care.delta_claims_transcations dct
# MAGIC ON dp.id = dct.PATIENTID
# MAGIC INNER JOIN
# MAGIC   lakehouse_dev.health_care.delta_allergies da
# MAGIC ON dp.id = da.PATIENT

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
# spark = SparkSession.builder.appName("Healthcare Data Join").getOrCreate()

# Load the tables into DataFrames
delta_patients = spark.read.format("delta").table("lakehouse_dev.health_care.delta_paitents")
delta_claims_transactions = spark.read.format("delta").table("lakehouse_dev.health_care.delta_claims_transcations")
delta_allergies = spark.read.format("delta").table("lakehouse_dev.health_care.delta_allergies")

# Check the schema of the delta_patients DataFrame
delta_patients.printSchema()

# Check the schema of the delta_claims_transactions DataFrame
delta_claims_transactions.printSchema()

# Check the schema of the delta_allergies DataFrame
delta_allergies.printSchema()

# COMMAND ----------

# Perform the joins
result = delta_patients \
    .join(delta_claims_transactions, delta_patients.Id == delta_claims_transactions.PATIENTID, "inner") \
    .join(delta_allergies, delta_patients.Id == delta_allergies.PATIENT, "inner")

# Display the result
display(result)

# COMMAND ----------

from pyspark.sql.functions import broadcast

# Perform the joins
result = delta_claims_transactions \
    .join(broadcast(delta_patients), delta_patients.Id == delta_claims_transactions.PATIENTID, "inner") \
    .join(broadcast(delta_allergies), delta_patients.Id == delta_allergies.PATIENT, "inner")

# Display the result
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC https://www.sqlshack.com/implementing-slowly-changing-dimensions-scds-in-data-warehouses/
# MAGIC https://docs.delta.io/latest/delta-update.html

# COMMAND ----------


