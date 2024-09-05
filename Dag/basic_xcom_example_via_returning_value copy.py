from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
 
from datetime import datetime
 
def _t1():
    return 42
 
def _t2(ti):
    value = ti.xcom_pull(key='my_key', task_ids='t1')
    print(value)
 
with DAG("basic_xcom_example_via_returning_value_dag", start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False, tags=['xcoms']) as dag:
 
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )
 
    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )
 
    t3 = BashOperator(
        task_id='t3',
        bash_command="echo 'Bash Operator'"
    )
 
    t1 >> t2 >> t3