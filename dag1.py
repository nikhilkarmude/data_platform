from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pkg_resources

def print_packages():
    packages = [d for d in pkg_resources.working_set]
    for package in packages:
        print(package)
        
with DAG(
    "print_packages",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    print_packages = PythonOperator(
        task_id="print_packages",
        python_callable=print_packages,
    )
