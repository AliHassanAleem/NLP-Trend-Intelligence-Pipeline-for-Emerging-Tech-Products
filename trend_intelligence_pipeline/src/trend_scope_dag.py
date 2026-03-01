from airflow import DAG
from airflow.ops.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for production-grade robustness
default_args = {
    'owner': 'virk',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'trend_scope_pipeline',
    default_args=default_args,
    description='Automated TrendScope NLP Pipeline',
    schedule_interval=None,  # Manually triggerable
    catchup=False,
    tags=['nlp', 'dvc', 'assignment'],
) as dag:

    # 1. Scrape Data (Stage 1 & 2)
    scrape_data = BashOperator(
        task_id='scrape_data',
        bash_command='dvc repro fetch_v2',
    )

    # 2. Preprocess Data (Stage 3)
    preprocess_data = BashOperator(
        task_id='preprocess_data',
        bash_command='dvc repro preprocess',
    )

    # 3. Generate Features (Stage 4)
    generate_features = BashOperator(
        task_id='generate_features',
        bash_command='dvc repro featurize',
    )

    # 4. Compute Statistics (Stage 5)
    compute_stats = BashOperator(
        task_id='compute_statistics',
        bash_command='dvc repro report',
    )

    # 5. DVC Push to GitHub Storage
    dvc_push = BashOperator(
        task_id='dvc_push',
        bash_command='dvc push',
    )

    # Define Dependencies
    scrape_data >> preprocess_data >> generate_features >> compute_stats >> dvc_push