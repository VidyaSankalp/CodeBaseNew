# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE student (id INT, name STRING, age INT) TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO student VALUES(2,"Ravi","32")

# COMMAND ----------

# MAGIC %sql 
# MAGIC update student set age = 33 where id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from student where id = 2

# COMMAND ----------

# providing a starting version
df = spark.readStream.format("delta") \
  .option("readChangeFeed", "true") \
  .table("student")

# COMMAND ----------

df.writeStream.format("delta").option("checkpointLocation", "/Volumes/test/delta/delta_checkpoint").table("student_cdc")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from student_cdc
