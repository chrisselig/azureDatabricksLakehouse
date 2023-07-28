# Databricks notebook source
# MAGIC %md
# MAGIC # This Script is used to load data from the bronze delta lake tables and transform them into the gold (dimensions) tables

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import DateType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dimension Tables
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create gold_dimRider table
# MAGIC
# MAGIC

# COMMAND ----------

# Read in tables required for this dimension table

rider = spark.table("default.bronze_rider") 


# COMMAND ----------

rider.printSchema()

# COMMAND ----------

rider.display(5)

# COMMAND ----------

# Update table names
rider = rider.withColumnRenamed("_c0","rider_id") \
    .withColumnRenamed("_c1","first") \
    .withColumnRenamed("_c2","last") \
    .withColumnRenamed("_c3","address") \
    .withColumnRenamed("_c4","birthday") \
    .withColumnRenamed("_c5","account_start_date") \
    .withColumnRenamed("_c6","account_end_date") \
    .withColumnRenamed("_c7","is_member")

# COMMAND ----------

# Transform column types
rider = rider.withColumn("rider_id",col("rider_id").cast("integer"))
rider = rider.withColumn("birthday",col("birthday").cast("date"))
rider = rider.withColumn("account_start_date",col("account_start_date").cast("date"))
rider = rider.withColumn("account_end_date",col("account_end_date").cast("date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write gold_dimRider to table

# COMMAND ----------

# Write to table gold_dimRider
rider.write.format("delta").mode("overwrite") \
    .saveAsTable("default.gold_dimRider")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create gold_dimStation table

# COMMAND ----------

# Read in tables required for this dimension table

station = spark.table("default.bronze_station") 

# COMMAND ----------

station.display(3)

# COMMAND ----------

# Update column names
station = station.withColumnRenamed("_c0","station_id") \
    .withColumnRenamed("_c1","name") \
    .withColumnRenamed("_c2","latitude") \
    .withColumnRenamed("_c3","longitude")

# COMMAND ----------

# Update data types
station = station.withColumn("station_id",col("station_id").cast("integer"))
station = station.withColumn("latitude",col("latitude").cast("float"))
station = station.withColumn("longitude",col("longitude").cast("float"))

# COMMAND ----------

station.display(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write gold_dimStation to table

# COMMAND ----------

# Write to table gold_dimRider
station.write.format("delta").mode("overwrite") \
    .saveAsTable("default.gold_dimStation")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create gold_dimDate table

# COMMAND ----------

beginDate = '2018-01-01'
endDate = '2035-12-31'

spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as date") \
    .createOrReplaceTempView('dates')


# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold_dimDate
# MAGIC using delta
# MAGIC location 'default/gold_dimDate'
# MAGIC as 
# MAGIC select
# MAGIC   year(date) * 10000 + month(date) * 100 + day(date) as date_id,
# MAGIC   date,
# MAGIC   YEAR(date) AS year,
# MAGIC   MONTH(date) AS month_number,
# MAGIC   date_format(date, 'MMMM') AS month_name,
# MAGIC   DAY(date) AS day_number,
# MAGIC   dayofweek(date) AS day_of_week,
# MAGIC 	quarter(date) AS quarter
# MAGIC from dates
# MAGIC order by
# MAGIC   date
