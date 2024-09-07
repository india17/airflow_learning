from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
from airflow.models import Variable
from scripts.bharat_kanwar.incremental_backup import incremental_etl
from scripts.bharat_kanwar.etl_functions import (
    late_payment_aggregated_view,
    billing_amount_aggregated_view,
)
from airflow.utils.task_group import TaskGroup

# import pendulum

from airflow.utils.dates import days_ago


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# PostgreSQL connection parameters
POSTGRESQL_CONFIG = {
    "host": "telecom.c2b3frbss0vu.ap-south-1.rds.amazonaws.com",
    "user": "postgres",
    "password": "Telecom_Airflow123",
    "database": "wetelco_telecom",
    "port": 5432,
}

DESTINATION_BUCKET = "tredence-backup-bucket/bharat"

# Define the default arguments
default_args = {
    "owner": "airflow",  # Owner of the DAG
    "start_date": days_ago(1),
    "email": ["burhanuddin@mentorskool.com"],  # List of emails to send notifications
    "email_on_retry": True,  # Send email on retry
    "email_on_failure": True,  # Send email on failure
    "retries": 2,  # Number of retries
    "retry_delay": timedelta(minutes=1),  # Delay between retries
    "schedule_interval": "@daily",  # How often to run the DAG
    "depends_on_past": False,  # Task doesn't depend on previous runs
    "end_date": None,  # No specific end date
    "retry_exponential_backoff": False,  # Don't increase delay between retries exponentially
    "max_retry_delay": timedelta(minutes=10),  # Maximum delay between retries
    "execution_timeout": timedelta(hours=2),  # Maximum execution time for the task
    "catchup": False,  # Whether to catch up on missed runs
}

# Instantiate the DAG
with DAG(
    dag_id="bharat_etl_task_group_hands_on",
    default_args=default_args,
    schedule_interval="30 4 * * 1-5",  # we want it to run on weekdays at 10 AM IST
    catchup=False,
    tags=['task-groups']
) as dag:

    # Incremental Backup Task Group
    with TaskGroup("incremental_backup") as incremental_backup:
        billing = PythonOperator(
            task_id="process_billing",
            python_callable=incremental_etl,
            op_args=["billing", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        customer_information = PythonOperator(
            task_id=f"process_customer_information",
            python_callable=incremental_etl,
            op_args=["customer_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        device_information = PythonOperator(
            task_id=f"process_device_information",
            python_callable=incremental_etl,
            op_args=["device_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        plans = PythonOperator(
            task_id=f"process_plans",
            python_callable=incremental_etl,
            op_args=["plans", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

        customer_rating = PythonOperator(
            task_id=f"process_customer_rating",
            python_callable=incremental_etl,
            op_args=["customer_rating", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
        )

    ## Factual Tasks
    AGGREGATED_VIEW_BUCKET = "tredence-aggregated-view-bucket/bharat"
    late_payment_aggregated_view_task = PythonOperator(
        task_id=f"late_payment_aggregated_view",
        python_callable=late_payment_aggregated_view,
        op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
    )

    billing_amount_aggregated_view_task = PythonOperator(
        task_id=f"billing_amount_aggregated_view",
        python_callable=billing_amount_aggregated_view,
        op_args=[DESTINATION_BUCKET, AGGREGATED_VIEW_BUCKET],
    )

    # Define task dependencies
    incremental_backup >> [
        late_payment_aggregated_view_task,
        billing_amount_aggregated_view_task,
    ]
