from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dir_structure_dag',
    default_args=default_args,
    description='A simple directory structure printing DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_cwd',
        bash_command='echo "Current Working Directory: $(pwd)"',
    )

    t2 = BashOperator(
        task_id='print_structure',
        bash_command='echo "Directory Structure:" && find /path/to/your/directory -print', # Replace '/path/to/your/directory' with the directory you want to print
    )

t1 >> t2
