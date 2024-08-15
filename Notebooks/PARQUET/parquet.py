# Databricks notebook source
def read_data(spark, bucket_name,folder_prefix):
    return spark.read.csv(f"s3://{bucket_name}/dataset/{folder_prefix}", header=True, inferSchema=True)

# COMMAND ----------

def write_data(df,bucket_name,database_name,write_mode,folder_prefix):
    # df.write.format("parquet").save(f"s3://prudhvi-08052024-test/dataset/parquet/{folder_prefix}")
    return df.write.format("parquet").mode(write_mode).option("path",f"s3://{bucket_name}/dataset/parquet/{folder_prefix}").saveAsTable(f"{database_name}.{folder_prefix}")

# COMMAND ----------

folder_prefixs = ["allergies","claims_transcations","claims","paitents","payers"]

source_bucket_name = 'prudhvi-08052024-test'
destination_bucket_name = 'prudhvi-08052024-test'
catalog_name = 'lakehouse_dev'
schema_name = 'health_care'
write_mode = "append"

database_name = f"{catalog_name}.{schema_name}"

for folder_prefix in folder_prefixs:
    df = read_data(spark, source_bucket_name,folder_prefix)
    write_data(df, destination_bucket_name,database_name,write_mode,folder_prefix)
