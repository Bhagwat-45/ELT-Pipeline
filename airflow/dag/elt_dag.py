from datetime import datetime, timedelta
from airflow import DAG
from docker.types import Mount
from airflow.operators.python import PythonOperator   # fixed import
from airflow.providers.docker.operators.docker import DockerOperator
import subprocess
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def run_elt_script():
    script_path = "/opt/airflow/elt/elt_script.py"
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script failed with error: {result.stderr}")
    print(result.stdout)

dag = DAG(
    'elt_and_dbt',
    default_args=default_args,
    description='An ELT workflow with dbt',
    start_date=datetime(2026, 3, 26),
    catchup=False,
)

t1 = PythonOperator(
    task_id="run_elt_script",
    python_callable=run_elt_script,
    dag=dag,
)

t2 = DockerOperator(
    task_id="dbt_run",
    image='ghcr.io/dbt-labs/dbt-postgres:latest',
    command=[
        "run",
        "--profiles-dir",   # fixed: was --profile-dir
        "/root",
        "--project-dir",
        "/dbt",
        "--full-refresh",
    ],
    auto_remove=True,
    docker_url="unix:///var/run/docker.sock",  # fixed: triple slash
    network_mode="elt_network",                 # match your compose network
    mounts=[
        Mount(source='/absolute/path/to/custom_postgres', target='/dbt', type='bind'),   # fixed comma + path
        Mount(source='/absolute/path/to/.dbt', target='/root', type='bind'),
    ],
    dag=dag,
)

t1 >> t2