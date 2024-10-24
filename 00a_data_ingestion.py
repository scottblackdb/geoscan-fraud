# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion Financial Transactions
# MAGIC Read in a csv file of financial transactions that include latitude and longitude of the transaction save the data to the Lakehouse

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read in the csv data and convert the geospatial and financial data from string to float data type

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
transactions = pd.read_csv('data/transactions.csv')
transactions['latitude'] = transactions['latitude'].apply(lambda x: float(x))
transactions['longitude'] = transactions['longitude'].apply(lambda x: float(x))
transactions['amount'] = transactions['amount'].apply(lambda x: float(x))
points_df = spark.createDataFrame(transactions)
display(points_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Save the cleansed data set to the Lakehouse

# COMMAND ----------

points_df.write.mode("overwrite").saveAsTable("transactions")
