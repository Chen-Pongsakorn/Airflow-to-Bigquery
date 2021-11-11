from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests

def get_data_from_api():
    url_1 = "https://covid19.ddc.moph.go.th/api/Cases/round-1to2-all"
    url_2 = "https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all"
    response_1 = requests.get(url_1)
    response_2 = requests.get(url_2)
    result_1 = response_1.json()
    result_2 = response_2.json()

    df_1 = pd.DataFrame.from_dict(result_1)
    df_2 = pd.DataFrame.from_dict(result_2)
    df_final = pd.concat([df_1,df_2], ignore_index=True)
    df_final = df_final.drop(columns=['update_date'])

    df_final.to_csv("/home/airflow/gcs/data/covid_from_api.csv", index=False)

# Default Args

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@once',
}

# Create DAG

dag = DAG(
    'Covid19_pipeline2bq',
    default_args=default_args,
    description='Pipeline for COVID-19',
    schedule_interval=timedelta(days=1),
)

# api_call
t1 = PythonOperator(
    task_id='api_call',
    python_callable=get_data_from_api,
    dag=dag,
)

# load to BigQuery
t2 = BashOperator(
    task_id='bq_load',
    bash_command='bq load --source_format=CSV --autodetect \
            [DATASET_ID].[TABLE_NAME] \
            gs://[GCS_BUCKET_NAME]/data/covid_from_api.csv',
    dag=dag,
)

# Dependencies

t1 >> t2
