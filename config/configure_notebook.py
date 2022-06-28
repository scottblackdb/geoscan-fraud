# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

import yaml
with open('config/application.yaml', 'r') as f:
  config = yaml.safe_load(f)

# COMMAND ----------

dbutils.fs.mkdirs(config['database']['path'])
_ = sql("CREATE DATABASE IF NOT EXISTS {} LOCATION '{}'".format(
  config['database']['name'], 
  config['database']['path']
))

# COMMAND ----------

# use our newly created database by default
# each table will be created as a MANAGED table under this directory
_ = sql("USE {}".format(config['database']['name']))

# COMMAND ----------

import mlflow
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment('/Users/{}/geoscan_experiment'.format(username))

# COMMAND ----------

def teardown():
  _ = sql("DROP DATABASE IF EXISTS {} CASCADE".format(config['database']['name']))
  dbutils.fs.rm(config['database']['path'], True)