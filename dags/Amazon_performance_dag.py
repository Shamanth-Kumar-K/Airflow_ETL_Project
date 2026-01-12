from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add sprint_3 folder to Python path
SPRINT_3_PATH = os.path.join(os.getcwd(), "sprint_3")
sys.path.append(SPRINT_3_PATH)

# Import sprint-3 functions
from sprint_3.vectorization import run_vectorization
from sprint_3.chunking import run_chunking
from sprint_3.memory_optimization import run_memory_optimization
from sprint_3.parallel_transformation import run_parallel_transformation
from sprint_3.sql_optimization import run_sql_optimization

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="amazon_performance_optimization",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Sprint 3 Performance Optimization on Amazon Ecommerce Dataset",
) as dag:

    vectorization_task = PythonOperator(
        task_id="vectorization_task",
        python_callable=run_vectorization,
    )

    chunking_task = PythonOperator(
        task_id="chunking_task",
        python_callable=run_chunking,
    )

    memory_task = PythonOperator(
        task_id="memory_optimization_task",
        python_callable=run_memory_optimization,
    )

    parallel_task = PythonOperator(
        task_id="parallel_transformation_task",
        python_callable=run_parallel_transformation,
    )

    sql_task = PythonOperator(
        task_id="sql_optimization_task",
        python_callable=run_sql_optimization,
    )

    # Task order
    vectorization_task >> chunking_task >> memory_task >> parallel_task >> sql_task
