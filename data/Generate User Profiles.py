# Databricks notebook source
!pip install faker

# COMMAND ----------

import pandas as pd

df = pd.read_csv("transactions.csv")

display(df)


# COMMAND ----------

from faker import Faker

fake = Faker()

df['first_name'] = [fake.first_name() for _ in range(len(df))]
df['last_name'] = [fake.last_name() for _ in range(len(df))]
df['ssn'] = [fake.ssn() for _ in range(len(df))]

df = df.drop(columns=['latitude', 'longitude', 'amount'])

display(df)

# COMMAND ----------

dfs = spark.createDataFrame(df)

dfs.write.mode("overwrite").saveAsTable("scott.default.user_profiles")
