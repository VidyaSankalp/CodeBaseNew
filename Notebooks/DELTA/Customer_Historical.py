# Databricks notebook source
# MAGIC %md
# MAGIC CREATE TABLE `customer` (
# MAGIC   `Id` int NOT NULL AUTO_INCREMENT,
# MAGIC   `firstname` varchar(240) DEFAULT NULL,
# MAGIC   `lastname` varchar(240) DEFAULT NULL,
# MAGIC   `effectiveDate` date DEFAULT NULL,
# MAGIC   `updateon` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
# MAGIC   `address` varchar(240) DEFAULT NULL,
# MAGIC   PRIMARY KEY (`Id`)
# MAGIC ) ENGINE=InnoDB AUTO_INCREMENT=301 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE TABLE `tables_info` (
# MAGIC   `table_name` varchar(250) DEFAULT NULL,
# MAGIC   `next_run_time` timestamp NULL DEFAULT NULL
# MAGIC ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
# MAGIC

# COMMAND ----------

connection_details ={
    "user": "admin",
    "password": "Root#123",
    "driver": "com.mysql.cj.jdbc.Driver",
    'partitionColumn':"Id",
    'lowerBound':str(1),
    'upperBound':str(100),
    'numPartitions':str(4)
}

jdbc_url = 'jdbc:mysql://database-2.chas42ia4ujd.ap-south-1.rds.amazonaws.com/employees'

jdbc_df = spark.read.jdbc(url=jdbc_url, table= f"(select * from customer) as foo",properties=connection_details)

# COMMAND ----------

# MAGIC %md
# MAGIC install highligted libraries
# MAGIC # ![](/Workspace/Users/xyzprudhvi@gmail.com/CodeBaseNew/Notebooks/DELTA/libraries.png)

# COMMAND ----------

display(jdbc_df)

# COMMAND ----------

from pyspark.sql.functions import col,explode,max
rows = jdbc_df.select(max('updateon').alias('max_date_time')).collect()
max_timestamp = rows[0][0]
print(max_timestamp)

# COMMAND ----------

from pyspark.sql.functions import lit

jdbc_df\
.withColumn('endDate',lit(''))\
.withColumn('current_status',lit(True))\
.write.mode("overwrite")\
.format("delta")\
.option("path","s3://delta-09042024/customer/")\
.saveAsTable("test.delta.customer")

# COMMAND ----------

from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote, quote
engine = create_engine('mysql+pymysql://admin:{}@database-2.chas42ia4ujd.ap-south-1.rds.amazonaws.com/employees'.format(quote('Root#123')), echo=False)

# COMMAND ----------

table_found = False
current_process_timestamp = ''
table_name_table_info = "employees.tables_info"
statement = "SELECT * from {} where table_name = '{}'".format(table_name_table_info,'customer')
print(statement)
result = engine.execute(statement)
results = result.fetchall()
if len(results) > 0:
    print('Table found')
    table_found = True
    current_process_timestamp = results[0][1]
else:
    engine.execute("INSERT INTO {} VALUES('{}',null)".format(table_name_table_info,'customer'))
    print('Table not found')

# COMMAND ----------

table_name = "employees.tables_info"
engine.execute("update {} set next_run_time='{}' where table_name='{}'".format(table_name,max_timestamp,'customer'))

# COMMAND ----------


