from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import requests
from requests.auth import HTTPBasicAuth
import time
from datetime import datetime
from airflow.operators.bash_operator import BashOperator

# Airbyte credentials from Airflow Variables
AIRBYTE_BASE_URL = Variable.get("airbyte_url")
AIRBYTE_USER_ID = Variable.get("airbyte_user_id")
AIRBYTE_PASSWORD = Variable.get("airbyte_password")
AIRBYTE_TAG = "jaffleshop"
AIRBYTE_CONN_URL = Variable.get("airbyte_conn_url") #airbyte_conn_url
#
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/opt/dbt"
DBT_PROFILE_DIR = "/opt/dbt"

# --- Helper: Get connections with a specific tag ---
def get_connections_by_tag(tag):
    url = AIRBYTE_CONN_URL  # Airbyte API endpoint
    headers = {"accept": "application/json"}
    
    response =  requests.get( url, headers=headers,auth=HTTPBasicAuth(AIRBYTE_USER_ID, AIRBYTE_PASSWORD))
    response.raise_for_status()
    all_connections = response.json().get("data", [])

    # Filter by tag
    tagged_connections = [
        conn["connectionId"]
        for conn in all_connections
        if tag in conn.get("name", "").lower() or tag in conn.get("tags", [])
    ]
    return tagged_connections


# --- Helper: Trigger sync ---
def trigger_airbyte_sync(AIRBYTE_CONNECTION_ID):
    headers = {
    "accept": "application/json",
    "content-type": "application/json"
    }   
    data = {
        "connectionId": AIRBYTE_CONNECTION_ID,
        "jobType": "sync"
    }
    #
    response = requests.post( AIRBYTE_BASE_URL, headers=headers, json=data, auth=HTTPBasicAuth(AIRBYTE_USER_ID, AIRBYTE_PASSWORD))
    #
    return response.json()["jobId"]


# --- Helper: Poll job status ---
def wait_for_sync(job_id, connection_id, poll_interval=10, timeout=900):
    """Poll job status until success or failure."""
    start_time = time.time()
    url = f"{AIRBYTE_BASE_URL}/{job_id}"
    headers = {"accept": "application/json"}
    data = {
        "connectionId": connection_id,
        "jobType": "sync"
    }

    in_progress_statuses = ["pending", "running", "queued"]
    
    while True:
        response = requests.get(url, headers=headers, json=data, auth=HTTPBasicAuth(AIRBYTE_USER_ID, AIRBYTE_PASSWORD))
        response.raise_for_status()
        status = response.json().get("status", "Unknown")
        print(f"Job Status: {status}")
        #
        if status.lower() == "succeeded":
            print(f"Job {job_id} completed successfully!")
            return "success"
        elif status in in_progress_statuses:
                print(f"Job {job_id} still in progress... Waiting...")
        elif status.lower() in ["failed", "error"]:
            return f"Job {job_id} failed!"
            
        if time.time() - start_time > timeout:
            return "Timeout reached while waiting for job success."
        poll_interval *= 2
        time.sleep(poll_interval)


# --- Task factory for Airflow ---
def make_airbyte_task(connection_id):
    @task(task_id=f"airbyte_sync_{connection_id}")
    def run_sync():
        job_id = trigger_airbyte_sync(connection_id)
        return wait_for_sync(job_id, connection_id)
    return run_sync


# --- Build the DAG ---
with DAG(
    dag_id="airbyte_jaffleshop_sync",
    start_date=datetime(2025, 8, 14),
    schedule_interval=None,
    catchup=False
) as dag:

    # 1. Get connection IDs
    connection_ids = get_connections_by_tag(AIRBYTE_TAG)

    # 2. Create a task for each connection
    sync_airbyte_tasks = [make_airbyte_task(conn_id)() for conn_id in connection_ids] 
    #
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}",
    )
    #
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    sync_airbyte_tasks >> dbt_run >> dbt_test