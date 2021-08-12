# Databricks notebook source
from pandas.io.json import json_normalize
import requests, json
import pandas as pd
from datetime import datetime
from datetime import timedelta
import dateutil.parser

# COMMAND ----------

# MAGIC %run /Release/secret_provider

# COMMAND ----------

# MAGIC %run "/Users/v-phclu@microsoft.com/Data Marshal/delta_lake_provider"

# COMMAND ----------

# MAGIC %run "/Users/v-phclu@microsoft.com/Data Marshal/marshal_provider"

# COMMAND ----------

adf_resource_group = 'datapipeline-{environment}'.format(environment=environment)
adf_resource_name = 't10etl-{environment}'.format(environment=environment)
adf_client_id = get_secret("adf-clientid")
adf_client_secret = get_secret("adf-clientsecret")
adf_api_version = "?api-version=2018-06-01"
adf_resource_url = 'https://management.core.windows.net/'
adf_pipelineRuns_url = 'https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{resource_name}/queryPipelineRuns{api_version}'.format(subscription_id=subscription_id, resource_group=adf_resource_group, resource_name=adf_resource_name, api_version=adf_api_version)
adf_post_data = {
  "lastUpdatedAfter": "2019-01-01T00:00:00.0000000Z",
  "lastUpdatedBefore": "2999-12-31T00:00:00.0000000Z",
  "orderBy": [
    {
      "order": "DESC",
      "orderBy": "RunStart"
    }
  ]
}

# COMMAND ----------

def get_adf_dataframe():
  results = api_post_json_response(adf_pipelineRuns_url, adf_post_data, adf_resource_url, adf_client_id, adf_client_secret)
  adf_dataframe = json_normalize(results,'value')
  adf_dataframe.fillna(0, inplace=True)
  adf_dataframe = adf_dataframe.drop(['annotations','runDimension'],axis=1)
  adf_dataframe = adf_dataframe.astype({"debugRunId": str, 
                                  "durationInMs": str,
                                  "invokedBy": str, 
                                  "isLatest": bool, 
                                  "lastUpdated": str, 
                                  "message": str, 
                                  "parameters": str,
                                  "pipelineName": str, 
                                  "runEnd": str, 
                                  "runGroupId": str, 
                                  "runId": str,
                                  "runStart": str,
                                  "status": str}, inplace=True)
  return adf_dataframe  

# COMMAND ----------

def save_adf_results_table(adf_dataframe):
  adf_spark_dataframe = spark.createDataFrame(adf_dataframe)
  adf_spark_dataframe = adf_spark_dataframe.withColumn('incident_id', lit(0))
  clear_delta_path(marshal_delta_file_path_adf)
  save_dataframe_to_delta_path(adf_spark_dataframe, "append", marshal_delta_file_path_adf)
  create_table_from_delta_path(marshal_delta_table_name_adf, marshal_delta_file_path_adf)
  merge_current_results_to_history(marshal_delta_table_name_adf, marshal_delta_table_name_adf_history, ['runId'], ['durationInMs', 'invokedBy', 'isLatest', 'lastUpdated', 'message', 'parameters', 'runEnd', 'runGroupId', 'runStart', 'status'], False)

# COMMAND ----------

def save_adf_icm_table(adf_icm_spark_dataframe):
  adf_icm_spark_dataframe = adf_icm_spark_dataframe[['runId','incident_id']]
  save_dataframe_to_delta_path(adf_icm_spark_dataframe, "overwrite", marshal_delta_file_path_adf_icm)
  create_table_from_delta_path(marshal_delta_table_name_adf_icm, marshal_delta_file_path_adf_icm)
  merge_current_results_to_history(marshal_delta_table_name_adf_icm, marshal_delta_table_name_adf_history, ['runId'], ['incident_id'], True)

# COMMAND ----------

def get_adf_results_filtered(adf_spark_dataframe, hours = 24):
  adf_dataframe = adf_spark_dataframe.toPandas()
  adf_dataframe['runStart'] = pd.to_datetime(adf_dataframe['runStart'])
  cutoff_date = adf_dataframe['runStart'].max() - pd.Timedelta(hours=hours)
  adf_dataframe = adf_dataframe[adf_dataframe['runStart'] > cutoff_date]
  return adf_dataframe

# COMMAND ----------

def show_adf_results(adf_spark_dataframe):
  adf_results_filtered = get_adf_results_filtered(adf_spark_dataframe)
  adf_results_filtered = adf_results_filtered[(adf_results_filtered['status'] != 'Succeeded')]
  displayHTML(generate_adf_report(adf_results_filtered))

# COMMAND ----------

def get_adf_failed_results():
  adf_history_spark_dataframe = spark.table(marshal_delta_table_name_adf_history)
  adf_history_dataframe = get_adf_results_filtered(adf_history_spark_dataframe)
  adf_history_dataframe = adf_history_dataframe[adf_history_dataframe.status == 'Failed']
  adf_history_dataframe = adf_history_dataframe[adf_history_dataframe.incident_id == 0]
  return adf_history_dataframe