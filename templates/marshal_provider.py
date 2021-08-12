# Databricks notebook source
from datetime import datetime
import pandas as pd
from pyspark.sql.functions import lit
import os

# COMMAND ----------

# MAGIC %run "/Users/v-phclu@microsoft.com/Data Marshal/delta_lake_provider"

# COMMAND ----------

environment = os.environ.get('KEY_VAULT_ENVIRONMENT')
subscription_id = os.environ.get('AZURE_SUBSCRIPTION_ID')
tenant_id = '72f988bf-86f1-41af-91ab-2d7cd011db47'
oath_token_url = 'https://login.microsoftonline.com/{tenant_id}/oauth2/token'.format(tenant_id=tenant_id)

# COMMAND ----------

def oath_get_access_token(resource, client_id, client_secret):
  data = {'grant_type': 'client_credentials', 'resource': resource}
  access_token_response = requests.post(oath_token_url, data=data, allow_redirects=False, auth=(client_id, client_secret))
  tokens = json.loads(access_token_response.text)
  access_token = tokens['access_token']
  return access_token

# COMMAND ----------

def api_get_json_response(api_uri, resource, client_id, client_secret):
  access_token = oath_get_access_token(resource, client_id, client_secret)
  api_call_headers = {'Authorization': 'Bearer ' + access_token}
  api_call_response = requests.get(api_uri, headers=api_call_headers)
  results = json.loads(api_call_response.text)
  return results

# COMMAND ----------

def api_post_json_response(api_uri, post_data, resource, client_id, client_secret):
  access_token = oath_get_access_token(resource, client_id, client_secret)
  api_call_headers = {'Authorization': 'Bearer ' + access_token, "Content-Type":"application/json"}
  api_call_response = requests.post(api_uri, data=json.dumps(post_data), headers=api_call_headers)
  results = json.loads(api_call_response.text)
  return results

# COMMAND ----------

def generate_html_table_headers(column_list, table_title):
  html_table_headers = "<table width='100%' style='font-family:Segoe UI; font-size:90%'>"
  html_table_headers += "<tr><td colspan=6 style='background-color: silver;'><b>{table_title}</b></td></tr>".format(table_title=table_title)
  html_table_headers += "<tr>"
  for column_name in column_list:
    html_table_headers += "<td><b>{column_name}</b></td>".format(column_name=column_name)
  html_table_headers += "</tr>"
  return html_table_headers    

# COMMAND ----------

def generate_adw_item_display(row, validation_name):
  html_adw_report = "<tr>"
  validation_name_color = 'black'
  if row['ValidationSource'] == "XBIStore": 
    validation_name_color = 'red'
  if row['ValidationSource'] == "T10Analytics": 
    validation_name_color = 'orange'
  html_adw_report += "<td valign=top style='border-bottom: 1px solid whitesmoke;'><font color={validation_name_color}><b>{validation_name}</b></td>".format(validation_name_color=validation_name_color, validation_name=validation_name)
  html_adw_report += "<td style='border-bottom: 1px solid whitesmoke;'>{date_id}</td>".format(date_id=str(row['DateId']))
  html_adw_report += "<td style='border-bottom: 1px solid whitesmoke;'>{date}</td>".format(date=str(row['Date']))
  html_adw_report += "<td style='border-bottom: 1px solid whitesmoke;'>{xuid_count}</td>".format(xuid_count=str(row['XuidCount']))
  html_adw_report += "</tr>"
  return html_adw_report

def generate_adw_report(adw_history_view_pandas_dataframe):
  html_adw_report = generate_html_table_headers(['Validation Name', 'Date Id', 'Date', 'Xuid Count'], 'ADF Table Validation')
  adw_history_view_pandas_dataframe = adw_history_view_pandas_dataframe.sort_values(by=['DateId', 'ValidationSource', 'ValidationType', 'ValidationObjectName'], ascending=True)
  for index, row in adw_history_view_pandas_dataframe.iterrows():
    validation_name = row['ValidationSource'] + ' | ' + row['ValidationType'] + ' | ' + row['ValidationObjectName']
    html_adw_report += generate_adw_item_display(row, validation_name)
  html_adw_report += "</table>"
  if len(adw_history_view_pandas_dataframe) == 0:
    html_adw_report = "<table width='100%' style='font-family:Segoe UI; font-size:90%'><tr><td>No Results to Display.</td></tr></table>"
  return html_adw_report

# COMMAND ----------

def generate_adf_item_display(row):
  html_adf_report = "<tr>"
  html_adf_report += "<td valign=top style='border-bottom: 1px solid whitesmoke;'><b>" + row['pipelineName'] + "</b></td>"
  status_color = 'black'
  if row['status'] == "Failed": 
    status_color = 'red'
  if row['status'] == "Cancelled": 
    status_color = 'orange'
  if row['status'] == "InProgress":
    status_color = 'green'
  html_adf_report += "<td style='border-bottom: 1px solid whitesmoke;'><font color=" + status_color + ">" + row['status'] + "</font></td>"
  html_adf_report += "<td style='border-bottom: 1px solid whitesmoke;'>" + row['runId'] + "</td>"
  start_stamp = row['runStart']
  html_adf_report += "<td style='border-bottom: 1px solid whitesmoke;'>" + datetime.strftime(start_stamp, '%Y-%m-%d %H:%M:%S.%f%z') + "</td>"
  html_adf_report += "<td style='border-bottom: 1px solid whitesmoke;'>"
  if row['runEnd'] is not None and row['runEnd'] != '0':
    end_stamp = dateutil.parser.parse(row['runEnd'])
    html_adf_report += datetime.strftime(end_stamp, '%Y-%m-%d %H:%M:%S.%f%z')
  html_adf_report += "</td>"
  html_adf_report += "<td style='border-bottom: 1px solid whitesmoke;'>"
  if row['durationInMs'] != "0.0":
    durationInMs = float(row['durationInMs'])
  else:
    durationDelta = datetime.today() - start_stamp
    durationInMs = (durationDelta.days * 86400000) + (durationDelta.seconds * 1000) + (durationDelta.microseconds / 1000)
  html_adf_report += str(timedelta(milliseconds=float(durationInMs)))
  html_adf_report += "</td>"
  html_adf_report += "<td style='border-bottom: 1px solid whitesmoke;'>" + row['invokedBy'] + "</td>"
  html_adf_report += "</tr>"
  if row['message'] != "":
    html_adf_report += "<tr><td></td><td valign=top colspan=6><table  style='font-family:Segoe UI; font-size:80%'><tr><td valign=top><div style='background-color: whitesmoke;'>" + row['message'] + "</div></td></tr></table></td>"
    html_adf_report += "<tr><td colspan=7>&nbsp;</td></tr>"
  return html_adf_report

def generate_adf_report(adf_pandas_dataframe):
  html_adf_report = generate_html_table_headers(['Pipeline Name', 'Status', 'Run Id', 'Run Start', 'Run End', 'Duration', 'Invoked By'], 'ADF Pipeline History')
  adf_pandas_dataframe = adf_pandas_dataframe.sort_values(by=['runStart'], ascending=False)
  for index, row in adf_pandas_dataframe.iterrows():
      html_adf_report += generate_adf_item_display(row)
  html_adf_report += "</table>"
  if len(adf_pandas_dataframe) == 0:
    html_adf_report = "<table width='100%' style='font-family:Segoe UI; font-size:90%'><tr><td>No Results to Display.</td></tr></table>"
  return html_adf_report

# COMMAND ----------

def generate_ssas_item_display(refresh_id, row):
  html_ssas_report = "<tr>"
  html_ssas_report += "<td valign=top style='border-bottom: 1px solid whitesmoke;'><b>" + refresh_id + "</b></td>"
  status_color = 'black'
  if row['status'] == "failed": 
    status_color = 'red'
  if row['status'] == "inProgress":
    status_color = 'green'
  html_ssas_report += "<td style='border-bottom: 1px solid whitesmoke;'><font color=" + status_color + ">" + row['status'] + "</font></td>"
  start_stamp = row['RefreshStartTimestamp']
  html_ssas_report += "<td style='border-bottom: 1px solid whitesmoke;'>" + datetime.strftime(start_stamp, '%Y-%m-%d %H:%M:%S.%f%z') + "</td>"
  html_ssas_report += "<td style='border-bottom: 1px solid whitesmoke;'>"
  if row['RefreshEndTimestamp'] is not None and row['RefreshEndTimestamp'] != '0':
    end_stamp = row['RefreshEndTimestamp']
    html_ssas_report += datetime.strftime(end_stamp, '%Y-%m-%d %H:%M:%S.%f%z')
  html_ssas_report += "</td>"
  html_ssas_report += "<td style='border-bottom: 1px solid whitesmoke;'>"
  durationInMs = row['RefreshDuration']
  html_ssas_report += str(durationInMs)
  html_ssas_report += "</td>"
  html_ssas_report += "<td style='border-bottom: 1px solid whitesmoke;'>" + row['currentRefreshType'] + "</td>"
  if str(row['objects']) != "":
    html_ssas_report += "<tr><td></td><td valign=top colspan=5><table  style='font-family:Segoe UI; font-size:80%'><tr><td valign=top><div style='background-color: whitesmoke;'>" + str(row['objects']) + "</div></td></tr></table></td>"
    html_ssas_report += "<tr><td colspan=6>&nbsp;</td></tr>"
  html_ssas_report += "</tr>"
  return html_ssas_report

def generate_ssas_report(ssas_refresh_df):
  html_ssas_report = generate_html_table_headers(['Refresh Id', 'Status', 'Refresh Start', 'Refresh End', 'Duration', 'Current Refresh Type'], 'SSAS Refresh History')
  ssas_refresh_df = ssas_refresh_df.sort_values(by=['RefreshStartTimestamp'], ascending=False)
  for index, row in ssas_refresh_df.iterrows():
      ssas_refresh_run_df = get_ssas_refreshId_dataframe(row['refreshId'])
      for index, run_row in ssas_refresh_run_df.iterrows():
        html_ssas_report += generate_ssas_item_display(row['refreshId'], run_row)
  html_ssas_report += "</table>"
  if len(ssas_refresh_df) == 0:
    html_ssas_report = "<table width='100%' style='font-family:Segoe UI; font-size:90%'><tr><td>No Results to Display.</td></tr></table>"
  return html_ssas_report