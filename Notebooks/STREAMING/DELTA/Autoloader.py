# Databricks notebook source
# MAGIC %md
# MAGIC https://docs.databricks.com/en/getting-started/etl-quick-start.html

# COMMAND ----------

file_path = "/Volumes/test/delta/auto_loader"
checkpoint_path = "s3://delta-09042024/autoloader_schema_location/"
source_df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
  .load(file_path)
)

# COMMAND ----------

from  pyspark.sql.functions import *
source_df\
.select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))\
.writeStream\
.format("delta")\
.option("mergeSchema","true")\
.outputMode("append")\
.option("checkpointLocation", "s3://delta-09042024/auto_loader_check_pointing1/")\
.toTable("test.delta.test_auto_loader1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test.delta.test_auto_loader1 where _rescued_data is  null
