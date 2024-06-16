from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import MongoClient
from datetime import datetime, timedelta


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Task 1: Drop Collection if it exist and then Create Collection
def create_collection_task():
    try:
        # Establish connection to MongoDB
        connection = MongoClient('mongodb://localhost:27017')
        # print(f"MongoDB connection - {connection.server_info()}")
        # Get the database
        db = connection.company
        # Get the collection "ops_employee"
        collection = db.ops_employees
        
        # # Connect to MongoDB
        # hook = MongoHook(mongodb_conn_id='mongo_default')
        
        # # collection = hook.get_collection(
        # #     mongo_db="company", mongo_collection="ops_employees"
        # # )
        
        # connection = hook.get_conn()
        # print(f"Connected to MongoDB - {connection.server_info()}")
        # # Get the database
        # db = connection.company
        # # Get the collection "ops_employee"
        # collection = db.ops_employee
                
        documents = collection.find({})

        for document in documents:
            print(document)
        
        # Delete the collection if exists
        if collection.count_documents({}) > 0:
            collection.drop()

        # Perform operations on the MongoDB data
        documents = [
            {
                'name': 'Ada Lovelace',
                'age': 205
            },
            {
                'name': 'Linus Torvals',
                'age': 54
            }
        ]

        # Set collection again
        collection = db.ops_employees

        # Load dataset into MongoDB
        collection.insert_many(documents)
    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")

#Create a DAG object and define its parameters:
with DAG(
    dag_id='mongodb_ops_employee_dag',
    default_args=default_args,
    description='Multi-step MongoDB DAG',
    schedule_interval='@daily'
) as dag:

    create_collection_operator = PythonOperator(
        task_id='create_collection',
        python_callable=create_collection_task,
        dag=dag
    )

    # Define task dependencies
    create_collection_operator
