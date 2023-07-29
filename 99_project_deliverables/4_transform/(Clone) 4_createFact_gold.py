# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact Tables

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import date_format, expr,months_between,current_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import needed tables for gold_factTrip

# COMMAND ----------

rider = spark.table("gold_dimRider")
trip = spark.table("bronze_trip")

# COMMAND ----------

rider.display(3)

# COMMAND ----------

trip.display(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform tables

# COMMAND ----------

# Updated column names for trip dataset
trip = trip.withColumnRenamed("_c0","trip_id") \
    .withColumnRenamed("_c1","rideable_type") \
    .withColumnRenamed("_c2","started_at") \
    .withColumnRenamed("_c3","ended_at") \
    .withColumnRenamed("_c4","start_station_id") \
    .withColumnRenamed("_c5","end_station_id") \
    .withColumnRenamed("_c6","member_id") 

trip.printSchema()

# COMMAND ----------

# Update data types
trip = trip.withColumn("started_at",col("started_at").cast("Timestamp"))
trip = trip.withColumn("ended_at",col("ended_at").cast("Timestamp"))
trip = trip.withColumn("start_station_id",col("start_station_id").cast("integer"))
trip = trip.withColumn("end_station_id",col("end_station_id").cast("integer"))
trip = trip.withColumn("member_id",col("member_id").cast("integer"))

# COMMAND ----------

# Combine tables to create the gold_factTrip table

# COMMAND ----------

trip.printSchema()

# COMMAND ----------

factTrip = trip.join(rider,trip.member_id == rider.rider_id,"left")

# COMMAND ----------

factTrip.printSchema()

# COMMAND ----------

# Create date_id column
factTrip = factTrip.withColumn("date_id", expr("date_format(started_at, 'yyyyMMdd')  "))

# COMMAND ----------

# Create duration column
factTrip = factTrip.withColumn('duration',col("ended_at").cast("long") - col('started_at').cast("long")) \
        .withColumn('rider_age',(months_between(current_date(), col('birthday')) / 12).cast('int'))

# COMMAND ----------

# Remove unwanted columns
factTrip = factTrip.select(col('trip_id'),col('date_id'),col('rider_id'),col('start_station_id'),col('end_station_id'),col('duration'),col('rider_age'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write gold_factTrip to table

# COMMAND ----------

# Write to table gold_dimRider
factTrip.write.format("delta").mode("overwrite") \
    .saveAsTable("default.gold_factTrip")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import tables to create gold_factPayment

# COMMAND ----------

payment = spark.table("bronze_payment")
dimdate = spark.table("gold_dimdate")

# COMMAND ----------

payment.display(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Tables

# COMMAND ----------

# Change column names
payment = payment.withColumnRenamed("_c0","payment_id") \
    .withColumnRenamed("_c1","date") \
    .withColumnRenamed("_c2","amount") \
    .withColumnRenamed("_c3","account_number")

# COMMAND ----------

# Change data types
payment = payment.withColumn("payment_id",col("payment_id").cast("integer"))
payment = payment.withColumn("date",col("date").cast("date"))
payment = payment.withColumn("amount",col("amount").cast("float"))
payment = payment.withColumn("account_number",col("account_number").cast("integer"))

# COMMAND ----------

payment.printSchema()

# COMMAND ----------

# Join rider table to payment table
factPayment = payment.join(rider,payment.account_number == rider.rider_id,"left")

# COMMAND ----------

# Join date table to previously created table
factPayment = factPayment.join(dimdate,factPayment.date == dimdate.date,"left")

# COMMAND ----------

# Select only needed columns
factPayment = factPayment.select("payment_id","rider_id","date_id","amount")

# COMMAND ----------

factPayment.display(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write gold_factPayment to table

# COMMAND ----------

# Write to table gold_dimRider
factPayment.write.format("delta").mode("overwrite") \
    .saveAsTable("default.gold_factPayment")
