# Databricks notebook source
# MAGIC %pip install azure-identity azure-storage-blob

# COMMAND ----------

from databricks.sdk.runtime import dbutils, display, spark

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from cdm_to_delta.model import (
    Environment,
    CdmManifest,
)
from cdm_to_delta.jobs import CdmToDeltaIngestionJob, CdmToParquetIngestionJob  # noqa: F401

# Credentials - specify a Unity Catalog service credential here
# https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-services/service-credentials#create-a-service-credential-using-a-managed-identity
service_credential_name = "oneenv-service"

# Storage details
account_name = "cdmfiles"
source_container_name = "cdm"
target_container_name = "cdm"

# this is the path that contains the model.json and the entities folder with csv files
cdm_root_path = "/Volumes/vuongnguyen/cdm/cdm/2024-10-24T14.49.38Z"
incremental_csv_container_path = "/Volumes/vuongnguyen/cdm/cdm"

# this is the target path where we will write the parquet files to
parquet_destination_root_path = "/Volumes/vuongnguyen/cdm/cdm/parquet"
log_schema = "vuongnguyen.cdm"
table_schema = "vuongnguyen.cdm"

entities = ["account"]

environment = Environment(
    service_credential_name=service_credential_name,
    source_account_name=account_name,
    source_container_name=source_container_name,
    target_account_name=account_name,
    target_container_name=target_container_name,
    cdm_root_path=cdm_root_path,
    log_schema_name=log_schema,
    incremental_csv_container_path=incremental_csv_container_path,
    parquet_destination_root_path=parquet_destination_root_path,
    delta_destination_schema=table_schema,
)

# COMMAND ----------

# 1. Read model.json
manifest = CdmManifest(environment, entities)

# COMMAND ----------

# 2. Init job object
ingestion_job = CdmToDeltaIngestionJob(spark, environment)

# COMMAND ----------

ingestion_job.copy_cdm_entities_to_destination(
    entities=manifest.get_entities().values(), update_log=True, mode=CdmToParquetIngestionJob.MODE_APPEND
)

# COMMAND ----------

if ingestion_job.log_entries:
    display(ingestion_job.log_entries)
