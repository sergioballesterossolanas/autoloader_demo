# Databricks notebook source
# MAGIC %run ./common

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df = (
    # Since this is a streaming source, this table is incremental.
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", path_checkpoint.replace("/dbfs", ""))
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(path.replace("/dbfs", ""))
  )

df.display()

# COMMAND ----------

