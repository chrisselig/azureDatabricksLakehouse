# Databricks notebook source
# MAGIC %md
# MAGIC # Take data from DBFS and add it to Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read payment data from Azure Databricks DBFS 

# COMMAND ----------

# Connect to payment data
path = "/FileStore/DivvybikeSharing/payments.csv"

payment = spark.read.format("csv") \
    .option('sep',',') \
    .option('header',False) \
    .load(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write payment data to delta lake

# COMMAND ----------

payment.write \
  .format("delta") \
  .mode("overwrite") \
  .option("mergeSchema","true") \
  .save("/delta/payment")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read in rider data from DBFS

# COMMAND ----------

rider = spark.read.format("csv") \
    .option('sep',',') \
    .option('header',False) \
    .load('/FileStore/DivvybikeSharing/riders.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write riders data to delta lake

# COMMAND ----------

rider.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/delta/rider")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read in Station data from DBFS

# COMMAND ----------

station = spark.read.format("csv") \
    .option("header","False") \
    .option("sep",",") \
    .load("/FileStore/DivvybikeSharing/stations.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Station data to Delta lake

# COMMAND ----------

station.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/delta/station")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read in the Trips data from DBFS

# COMMAND ----------

trip = spark.read.format("csv") \
    .option("sep",",") \
    .option("header","False") \
    .load("/FileStore/DivvybikeSharing/trips.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write trip data to Delta Lake

# COMMAND ----------

trip.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/delta/trip")
