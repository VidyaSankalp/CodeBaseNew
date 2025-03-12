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

connection_details = {
    "user": "admin",
    "password": "Root#123",
    "driver": "com.mysql.cj.jdbc.Driver",
    'partitionColumn': "Id",
    'lowerBound': "301",
    'upperBound': "500",
    'numPartitions': "4"
}

jdbc_url = 'jdbc:mysql://database-2.cv0g6mqwqu0z.ap-south-1.rds.amazonaws.com:3306/employee'

jdbc_df = spark.read.jdbc(
    url=jdbc_url,
    table="(select * from customer) as foo",
    properties=connection_details
)

display(jdbc_df)

# COMMAND ----------

jdbc_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,explode,max
rows = jdbc_df.select(max('updateon').alias('max_date_time')).collect()
max_timestamp = rows[0][0]
print(max_timestamp)

# COMMAND ----------

from pyspark.sql.functions import lit

jdbc_df\
.withColumn('endDate',lit(None).cast("date"))\
.withColumn('current_status',lit(1))\
.write.mode("overwrite")\
.format("delta")\
.option("path","s3://prudhvi-test-destination-02272025/customer/")\
.saveAsTable("lakehouse.test.customer")

# COMMAND ----------

from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote, quote
engine = create_engine('mysql+pymysql://admin:{}@database-2.cv0g6mqwqu0z.ap-south-1.rds.amazonaws.com/employee'.format(quote('Root#123')), echo=False)

# COMMAND ----------

from sqlalchemy import text

table_found = False
current_process_timestamp = ''
table_name_table_info = "employee.tables_info"
statement = text("SELECT * from {} where table_name = 'customer'".format(table_name_table_info))
print(statement)

with engine.connect() as connection:
    result = connection.execute(statement)
    results = result.fetchall()
    if len(results) > 0:
        print('Table found')
        table_found = True
        current_process_timestamp = results[0][1]
    else:
        insert_statement = f"INSERT INTO {table_name_table_info} VALUES('customer','{max_timestamp}')"
        print(insert_statement)
        connection.execute(text(insert_statement))
        connection.commit()
        print('Table not found')

# COMMAND ----------

if table_found:
    with engine.connect() as connection:
        table_name = "employee.tables_info"
        connection.execute("update {} set next_run_time='{}' where table_name='{}'".format(table_name,max_timestamp,'customer'))
        connection.commit()
