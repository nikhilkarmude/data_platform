from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

# Define the Python function
def task_with_exception_logging(**kwargs):
    try:
        # Replace this with the actual logic for your task
        raise ValueError('This is a simulated exception for demonstration purposes.')
    except Exception as e:
        # Log the error in the specified format
        logging.error({
            'Exception_DETAILS': str(e),
            'ERROR_CATEGORY': 'DEV:DQ:DQ_REJECTS'
        })
        # Re-raise the exception if you want the task to be marked as failed.
        raise

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('exception_logging_dag',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False)

# Define the task
log_exception_task = PythonOperator(
    task_id='log_exception_task',
    python_callable=task_with_exception_logging,
    provide_context=True,
    dag=dag,
)

# Set the task sequence
log_exception_task

# If you want to run this DAG, don't forget to save it in the Airflow DAGs directory
