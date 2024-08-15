# Databricks notebook source
def read_data(spark, bucket_name, folder_prefix):
    # Reads data from a specified S3 bucket and folder prefix, inferring schema and using the first row as headers
    return spark.read.csv(f"s3://{bucket_name}/dataset/{folder_prefix}", header=True, inferSchema=True)

# COMMAND ----------

def write_data(df, bucket_name, database_name, write_mode, folder_prefix):
    # Writes the DataFrame to a Parquet file in the specified S3 bucket and folder prefix
    # Parameters:
    # df: The DataFrame to write
    # bucket_name: The name of the S3 bucket where the data will be written
    # database_name: The name of the database to associate the table with
    # write_mode: The write mode (e.g., append, overwrite)
    # folder_prefix: The folder prefix within the bucket to write the data to
    
    # The data is written in Parquet format to the specified path and also saved as a table in the database
    return df.write.format("parquet").mode(write_mode).option("path", f"s3://{bucket_name}/dataset/parquet/{folder_prefix}").saveAsTable(f"{database_name}.{folder_prefix}")

# COMMAND ----------

# Define the list of folder prefixes to process
folder_prefixes = ["allergies", "claims_transactions", "claims", "patients", "payers"]

# Specify the source and destination S3 bucket names
source_bucket_name = 'prudhvi-08052024-test'
destination_bucket_name = 'prudhvi-08052024-test'

# Define the catalog and schema names for the database
catalog_name = 'lakehouse_dev'
schema_name = 'health_care'

# Set the write mode for saving data
write_mode = "append"

# Construct the full database name using the catalog and schema names
database_name = f"{catalog_name}.{schema_name}"

# Loop through each folder prefix to process the data
for folder_prefix in folder_prefixes:
    # Read data from the source bucket for the current folder prefix
    df = read_data(spark, source_bucket_name, folder_prefix)
    # Write the DataFrame to the destination bucket and register it as a table in the database
    write_data(df, destination_bucket_name, database_name, write_mode, folder_prefix)
