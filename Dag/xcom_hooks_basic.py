from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pendulum
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SQLToS3Operator


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024, 8, 26, tz="Asia/Kolkata"),
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
    "catchup": True,
    "email": ["burhanuddin@mentorskool.com", "amit@mentorskool.com"],
    "email_on_retry": True,
    "email_on_failure": True,
}


POSTGRES_CONN_ID = "telecom_airflow"
DESTINATION_BUCKET = "airflow-destination-data/burhan"


# define the python function to fetch data
def fetch_last_run_date(table, **kwargs):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = (
        f"SELECT last_run_date FROM etl_last_run_metadata WHERE table_name = '{table}';"
    )
    result = hook.get_first(sql)
    if result:
        last_run_date = result[0]
    else:
        last_run_date = "1970-01-01 00:00:00"  # Fallback date if no entry is found
    kwargs["ti"].xcom_push(key=f"last_run_date", value=last_run_date)


def print_last_run_date(table, **kwargs):
    last_run_date = kwargs["ti"].xcom_pull(
        key="last_run_date",
        task_ids=f"get_last_run_date_group.get_{table}_last_run_date",
    )
    print(f"Last run date of {table}: {last_run_date}")


with DAG(
    dag_id="hooks_and_xcoms_with_task_groups",
    start_date=days_ago(1),
    owner_links={"airflow": "https://airflow.apache.org/"},
    tags=["hooks", "xcoms"],
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
):
    with TaskGroup("get_last_run_date_group") as get_last_run_date_group:
        get_last_run_date_tasks = [
            PythonOperator(
                task_id=f"get_{table}_last_run_date",
                python_callable=fetch_last_run_date,
                op_args=[table],
            )
            for table in [
                "customer_information",
                "billing",
                "customer_rating",
                "device_information",
                "plans",
            ]
        ]

    with TaskGroup("print_last_run_date_group") as print_last_run_date_group:
        print_last_run_date_tasks = [
            PythonOperator(
                task_id=f"print_{table}_last_run_date",
                python_callable=print_last_run_date,
                op_args=[table],
            )
            for table in [
                "customer_information",
                "billing",
                "customer_rating",
                "device_information",
                "plans",
            ]
        ]

    # Set dependencies between the task groups
    get_last_run_date_group >> print_last_run_date_group
