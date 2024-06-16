from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging


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
    'postgres_it_employee_dag',
    default_args=default_args,
    description='Multi-step PostgreSQL DAG',
    schedule_interval='@daily',
)

# Task 1: Drop Table if it exist and then Create Table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql='''
    DROP TABLE IF EXISTS it_employee;
    CREATE TABLE it_employee (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50),
        age INT
    );
    ''',
    dag=dag,
)

# Branching: Decide which data to insert based on the day of the run
def determine_insert(**kwargs):
    execution_date = kwargs['execution_date']
    day = execution_date.day
    if day % 2 == 0:
        return 'insert_data_even'
    else:
        return 'insert_data_odd'
    
branch_task = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=determine_insert,
    dag=dag,
)

# Task 2a: Insert Data for Even Date
insert_data_even = PostgresOperator(
    task_id='insert_data_even',
    postgres_conn_id='postgres_default',
    sql='''
    INSERT INTO it_employee (name, age) VALUES
    ('Bjarne Stroustrup', 73),
    ('Mitchell Baker', 67);
    ''',
    dag=dag,
)

# Task 2b: Insert Data for Odd Date
insert_data_odd = PostgresOperator(
    task_id='insert_data_odd',
    postgres_conn_id='postgres_default',
    sql='''
    INSERT INTO it_employee (name, age) VALUES
    ('Radia Perlman', 72),
    ('Linus Torvalds', 54);
    ''',
    dag=dag,
)

# Join task to ensure downstream tasks execute
join_task = DummyOperator(
    task_id='join_task',
    trigger_rule='one_success',
    dag=dag,
)

# Task 3: Read Data and Log Results
def log_results():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    result = hook.get_records('SELECT * FROM it_employee;')
    for row in result:
        logging.info(row)

read_data = PythonOperator(
    task_id='log_read_data',
    python_callable=log_results,
    dag=dag,
)

# Define task dependencies
create_table >> branch_task
branch_task >> insert_data_even >> join_task
branch_task >> insert_data_odd >> join_task
join_task >> read_data
