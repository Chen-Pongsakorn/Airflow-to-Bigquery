import base64
import datetime
import os
import requests
from google.cloud import bigquery


class Config:
    dataset_id = os.environ.get("dataset_id")
    table_name = os.environ.get("table_name")
    url = os.environ.get("url")
    

def get_data(url):
    response = requests.get(url)
    result = response.json()

    rows = []
    rows.append((result[0]['txn_date'], 
                result[0]['new_case'],
                result[0]['total_case'],
                result[0]['new_case_excludeabroad'],
                result[0]['total_case_excludeabroad'],
                result[0]['new_death'],
                result[0]['total_death'],
                result[0]['new_recovered'],
                result[0]['total_recovered'],
                ))
    return rows

def insert_data(event, context):
    client = bigquery.Client()
    dataset_ref = client.dataset(Config.dataset_id)
    
    record = get_data(Config.url)

    table_ref = dataset_ref.table(Config.table_name) 
    table = client.get_table(table_ref)
    result = client.insert_rows(table, record)
    return result
    