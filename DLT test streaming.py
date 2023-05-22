# Databricks notebook source
# MAGIC %run ./common

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.create_table()
def streaming_bronze():
  return (
    # Since this is a streaming source, this table is incremental.
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaEvolutionMode", "rescue")
      .load(path)
  )


# COMMAND ----------


@dlt.table
def live_gold():
  # This table will be recomputed completely by reading the whole silver table
  # when it is updated.
  return dlt.read_stream("streaming_bronze").groupBy("City").count()