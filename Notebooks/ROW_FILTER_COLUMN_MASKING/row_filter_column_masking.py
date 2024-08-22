# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE FUNCTION us_filter(region STRING)
# MAGIC RETURN IF(IS_ACCOUNT_GROUP_MEMBER('admin'), true, region='US');
