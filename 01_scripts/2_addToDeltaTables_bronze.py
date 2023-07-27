# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook is used to ingest data from delta lake and write them to bronze tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the bronze_payment table from the Delta Lake data

# COMMAND ----------

spark.sql("CREATE TABLE bronze_payment USING DELTA LOCATION '/delta/payment'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the bronze_rider table from the Delta Lake data
# MAGIC

# COMMAND ----------

spark.sql("CREATE TABLE bronze_rider USING DELTA LOCATION '/delta/rider'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the bronze_station table from the delta lake data

# COMMAND ----------

spark.sql("CREATE TABLE bronze_station USING DELTA LOCATION '/delta/station'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the bronze_trip table from the delta lake data

# COMMAND ----------

spark.sql("CREATE TABLE bronze_trip USING DELTA LOCATION '/delta/trip'")
