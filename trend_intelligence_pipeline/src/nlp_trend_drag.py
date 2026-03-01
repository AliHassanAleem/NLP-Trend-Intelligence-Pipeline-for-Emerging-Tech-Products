from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from scraper import fetch_products  # But we'll call whole scripts
import subprocess  # To run scripts

def run_script(script_name):
    subprocess.run(["python", f"src/{script_name}"], check=True)

def dvc_push():
    subprocess.run(["dvc", "push"], check=True)

with DAG(
    dag_id='nlp_trend_dag',
    start_date=datetime(2026, 2, 28),
    schedule_interval=None,  # Manual trigger
    catchup=False
) as dag:
    scrape = PythonOperator(
        task_id='scrape_data',
        python_callable=lambda: run_script('scraper.py'),
        retries=3
    )
    preprocess = PythonOperator(
        task_id='preprocess_data',
        python_callable=lambda: run_script('preprocess.py'),
        retries=3
    )
    features = PythonOperator(
        task_id='generate_features',
        python_callable=lambda: run_script('representation.py'),
        retries=3
    )
    stats = PythonOperator(
        task_id='compute_statistics',
        python_callable=lambda: run_script('statistics.py'),
        retries=3
    )
    push = PythonOperator(
        task_id='dvc_push',
        python_callable=dvc_push,
        retries=3
    )

    scrape >> preprocess >> features >> stats >> push