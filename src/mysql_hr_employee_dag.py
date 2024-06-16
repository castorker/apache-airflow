from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
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
    'mysql_hr_employee_dag',
    default_args=default_args,
    description='Multi-step MySQL DAG',
    schedule_interval='@daily',
)

# Task 1: Drop Table if it exist and then Create Table
create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_default',
    sql='''
    DROP TABLE IF EXISTS hr_employee;
    CREATE TABLE hr_employee (
        id INT AUTO_INCREMENT PRIMARY KEY,
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
insert_data_even = MySqlOperator(
    task_id='insert_data_even',
    mysql_conn_id='mysql_default',
    sql='''
    INSERT INTO hr_employee (name, age) VALUES
    ('Bob Dylan', 83),
    ('Anna Calvi', 43);
    ''',
    dag=dag,
)

# Task 2b: Insert Data for Odd Date
insert_data_odd = MySqlOperator(
    task_id='insert_data_odd',
    mysql_conn_id='mysql_default',
    sql='''
    INSERT INTO hr_employee (name, age) VALUES
    ('Jessie Ware', 39),
    ('Neil Young', 78);
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
    hook = MySqlHook(mysql_conn_id='mysql_default')
    result = hook.get_records('SELECT * FROM hr_employee;')
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
