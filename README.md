# Airflow to Bigquery

#### Use airflow to insert data into bigquery

# How to run

1. Create BigQuery table with no schema setting.
2. Enable Composer Api and install python packages.
3. Place python file in dags folder that you recieve from environment configuration.
4. Check Airflow web UI for pipeline status.

# Cloud function to Bigquery

#### Use cloud function to stream data into bigquery for the update

# How to run

1. Create Cloud Function and select Pub/Sub as a trigger type.
2. Set runtime environment variables.
3. Add code and requirments.
4. Deploy Cloud Function.
5. Set Cloud Scheduler to run function everyday.

# Example data in Bigquery
![console](img/Example_data.png?raw=true)