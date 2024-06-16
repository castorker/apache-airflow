from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
dag = DAG(
    'cross_database_employee_reporting',
    default_args=default_args,
    description='Generate a report based on data from two different sources: MySQL and PostgreSQL databases',
    schedule_interval='@daily',
)

# Task 1: Extract data from MySQL source
def extract_mysql_data(**kwargs):
    try:
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        sql = 'SELECT * FROM hr_employee;'
        df = mysql_hook.get_pandas_df(sql)
        df.to_csv('/tmp/mysql_hr_employee_data.csv', index=False)
    except Exception as e:
        logging.error(f"Error extracting data from MySQL database: {e}")
        raise

extract_mysql_task = PythonOperator(
    task_id='extract_mysql_data',
    python_callable=extract_mysql_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Extract data from PostgreSQL source
def extract_postgres_data(**kwargs):
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = 'SELECT * FROM it_employee;'
        df = postgres_hook.get_pandas_df(sql)
        df.to_csv('/tmp/postgres_it_employee_data.csv', index=False)
    except Exception as e:
        logging.error(f"Error extracting data from PostgreSQL database: {e}")
        raise

extract_postgres_task = PythonOperator(
    task_id='extract_postgres_data',
    python_callable=extract_postgres_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Combine data and generate report
def generate_report(**kwargs):
    try:
        mysql_hr_employee_data = pd.read_csv('/tmp/mysql_hr_employee_data.csv')
        postgres_it_employee_data = pd.read_csv('/tmp/postgres_it_employee_data.csv')

        # Combine the data
        combined_employee_data = pd.concat([mysql_hr_employee_data, postgres_it_employee_data])

        # Log combined data
        logging.info(f"Combined Employee Data:\n{combined_employee_data}")

        # Generate report
        report = combined_employee_data.describe()

        # Save the combined data and report
        combined_employee_data.to_csv('/tmp/combined_employee_data.csv', index=False)
        report.to_csv('/tmp/employee_report.csv')
    except Exception as e:
        logging.error(f"Error generating report: {e}")
        raise

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag,
)

# Task 4: Log the report results
def log_report_results(**kwargs):
    try:
        combined_employee_data = pd.read_csv('/tmp/combined_employee_data.csv')
        employee_report = pd.read_csv('/tmp/employee_report.csv')

        logging.info(f"Combined Employee Data:\n{combined_employee_data}")
        logging.info(f"Employee Report:\n{employee_report}")
    except Exception as e:
        logging.error(f"Error logging report results: {e}")
        raise

log_report_task = PythonOperator(
    task_id='log_report_results',
    python_callable=log_report_results,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract_mysql_task >> extract_postgres_task >> generate_report_task >> log_report_task

# Dummy start task to ensure proper task flow
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Dummy end task to ensure proper task flow
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

start_task >> [extract_mysql_task, extract_postgres_task]
log_report_task >> end_task
