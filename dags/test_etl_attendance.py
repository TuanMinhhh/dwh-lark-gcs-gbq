from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pytz import timezone
import os 


# Import các hàm từ các file Python
from ingestion.lark_to_gcs import task1_lark_to_gcs


#
def print_text():
    print('Hello worlds')


# Định nghĩa default_args cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 25),
    'retries': 1
}


with DAG(
    dag_id='test_etl_attendance',
    default_args=default_args,
    description='Test airflow attendance',
    schedule_interval=None
) as dag:
    # Định nghĩa task đầu tiên: chạy lark_to_gcs.py
    task_lark_to_gcs = PythonOperator(
        task_id='run_lark_to_gcs',
        python_callable=task1_lark_to_gcs  # Chạy hàm từ file lark_to_gcs.py
    )

    # Định nghĩa task thứ hai: chạy gcs_to_gbq.py
    task_gcs_to_gbq = PythonOperator(
        task_id='print_text',
        python_callable=print_text  # Chạy hàm từ file gcs_to_gbq.py
    )

    # # Định nghĩa task thứ ba: chạy summarize_table.py
    # task_summarize_table = PythonOperator(
    #     task_id='run_summarize_table',
    #     python_callable=summarize_table  # Chạy hàm từ file summarize_table.py
    # )

    # Xác định thứ tự chạy: task_lark_to_gcs -> task_gcs_to_gbq -> task_summarize_table
    task_lark_to_gcs >> task_gcs_to_gbq #>> task_summarize_table

# if __name__ == "__main__":
#     dag.cli()
