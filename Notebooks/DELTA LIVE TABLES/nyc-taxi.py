# Databricks notebook source
# MAGIC %sql
# MAGIC -- Bronze layer: Raw data ingestion
# MAGIC CREATE OR REFRESH STREAMING TABLE taxi_raw_records 
# MAGIC (CONSTRAINT valid_distance EXPECT (trip_distance > 0.0) ON VIOLATION DROP ROW)
# MAGIC AS SELECT *
# MAGIC FROM STREAM(samples.nyctaxi.trips);
# MAGIC
# MAGIC -- -- Silver layer 1: Flagged rides
# MAGIC -- CREATE OR REFRESH STREAMING TABLE flagged_rides 
# MAGIC -- AS SELECT
# MAGIC --   date_trunc("week", tpep_pickup_datetime) as week,
# MAGIC --   pickup_zip as zip, 
# MAGIC --   fare_amount, trip_distance
# MAGIC -- FROM
# MAGIC --   STREAM(LIVE.taxi_raw_records)
# MAGIC -- WHERE ((pickup_zip = dropoff_zip AND fare_amount > 50) OR
# MAGIC --        (trip_distance < 5 AND fare_amount > 50));
# MAGIC
# MAGIC -- -- Silver layer 2: Weekly statistics
# MAGIC -- CREATE OR REFRESH MATERIALIZED VIEW weekly_stats
# MAGIC -- AS SELECT
# MAGIC --   date_trunc("week", tpep_pickup_datetime) as week,
# MAGIC --   AVG(fare_amount) as avg_amount,
# MAGIC --   AVG(trip_distance) as avg_distance
# MAGIC -- FROM
# MAGIC --  live.taxi_raw_records
# MAGIC -- GROUP BY week
# MAGIC -- ORDER BY week ASC;
# MAGIC
# MAGIC -- -- Gold layer: Top N rides to investigate
# MAGIC -- CREATE OR REPLACE MATERIALIZED VIEW top_n
# MAGIC -- AS SELECT
# MAGIC --   weekly_stats.week,
# MAGIC --   ROUND(avg_amount,2) as avg_amount, 
# MAGIC --   ROUND(avg_distance,3) as avg_distance,
# MAGIC --   fare_amount, trip_distance, zip 
# MAGIC -- FROM live.flagged_rides
# MAGIC -- LEFT JOIN live.weekly_stats ON weekly_stats.week = flagged_rides.week
# MAGIC -- ORDER BY fare_amount DESC
# MAGIC -- LIMIT 3;
