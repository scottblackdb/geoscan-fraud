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


if 'catalog' not in config['database']:

  dbutils.fs.mkdirs(config['database']['path'])
  _ = sql("CREATE DATABASE IF NOT EXISTS {} LOCATION '{}'".format(
    config['database']['name'], 
    config['database']['path']
  ))

  # use our newly created database by default
  # each table will be created as a MANAGED table under this directory
  _ = sql("USE {}".format(config['database']['name']))
else:
  ctlg = config['database']['catalog']
  db = config['database']['name']
  sql(f"CREATE CATALOG IF NOT EXISTS {ctlg}")
  sql(f"USE CATALOG {ctlg}")

  sql(f"CREATE DATABASE IF NOT EXISTS {db}")
  sql(f"USE DATABASE {db}")
  

# COMMAND ----------

import mlflow
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment('/Users/{}/geoscan'.format(username))

# COMMAND ----------

# Where we might stored temporary data on local disk
from pathlib import Path
temp_directory = f"/tmp/{username}/geoscan"
Path(temp_directory).mkdir(parents=True, exist_ok=True)

# COMMAND ----------

def teardown():
  _ = sql("DROP DATABASE IF EXISTS {} CASCADE".format(config['database']['name']))
  dbutils.fs.rm(config['database']['path'], True)
  import shutil
  shutil.rmtree(temp_directory)
