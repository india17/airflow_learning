from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
from airflow.models import Variable

from scripts.bharat_kanwar.incremental_backup import incremental_etl
from scripts.bharat_kanwar.etl_functions import (
    late_payment_aggregated_view,
    billing_amount_aggregated_view
)

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
    "email": ["bharat.kanwar@tredence.com.com"],  # List of emails to send notifications
    "email_on_retry": True,  # Send email on retry
    "email_on_failure": True,  # Send email on failure
    "retries": 2,  # Number of retries
    "retry_delay": timedelta(minutes=1),  # Delay between retries
    "schedule_interval": "@daily",  # How often to run the DAG
    "depends_on_past": False,  # Task doesn't depend on previous runs
    "end_date": None,  # No specific end date
    "retry_exponential_backoff": False,  # Don't increase delay between retries exponentially
    "max_retry_delay": timedelta(minutes=10),  # Maximum delay between retries
    "sla": timedelta(hours=1),  # Service Level Agreement for the task
    "execution_timeout": timedelta(hours=2),  # Maximum execution time for the task
    "queue": "default",  # Queue in which the task will be executed
    "priority_weight": 1,  # Priority weight for the task
    "wait_for_downstream": False,  # Don't wait for downstream tasks to finish
    "trigger_rule": "all_success",  # Trigger when all upstream tasks have succeeded
    "pool": "default_pool",  # Pool in which the task will be executed
    "catchup": False,  # Whether to catch up on missed runs
}

# Instantiate the DAG
with DAG(
    dag_id="bharat_etl_dag",
    default_args=default_args,
    schedule_interval="30 4 * * 1-5",  # we want it to run on weekdays at 10 AM IST
    catchup=False,
) as dag:

    billing = PythonOperator(
        task_id=f"process_billing",
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
    AGGREGATED_VIEW_BUCKET = "airflow-facts-analyses/burhan"
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
    customer_information >> [billing, device_information, customer_rating]
    plans >> billing_amount_aggregated_view_task
    customer_information >> billing_amount_aggregated_view_task
    billing >> late_payment_aggregated_view_task
