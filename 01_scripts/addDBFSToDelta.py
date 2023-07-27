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
# MAGIC stations, trips
