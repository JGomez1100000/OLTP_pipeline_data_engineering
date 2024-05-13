from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt

# Define DAG arguments
default_args = {
    'owner': 'admin',
    'start_date': dt.datetime(2024, 5, 14),
    'email': ['javiergomezbiedma@gmail.com'],
}

# Define DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='ETL pipeline webserver accesslog',
    schedule=dt.timedelta(days=1)
)

# Define tasks using BashOperator

# Task to extract data
extract_data = BashOperator(
    task_id='extract_data',
    bash_command='grep ".html" /venv/lib/python3.12/site-packages/airflow/dags/capstone_project/accesslog.txt | awk \'{print $1, $10}\' > /venv/lib/python3.12/site-packages/airflow/dags/capstone_project/extracted_data.txt',
    dag=dag
)

# Task to transform data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='grep -v "198.46.149.143" /venv/lib/python3.12/site-packages/airflow/dags/capstone_project/extracted_data.txt > /venv/lib/python3.12/site-packages/airflow/dags/capstone_project/transformed_data.txt',
    dag=dag
)

# Task to load data
load_data = BashOperator(
    task_id='load_data',
    bash_command='tar -cvf /venv/lib/python3.12/site-packages/airflow/dags/capstone_project/weblog.tar /venv/lib/python3.12/site-packages/airflow/dags/capstone_project/transformed_data.txt',
    dag=dag
)

# Define task dependencies
extract_data >> transform_data >> load_data
