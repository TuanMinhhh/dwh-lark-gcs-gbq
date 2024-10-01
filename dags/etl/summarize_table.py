import os
import pandas as pd
import pandas_gbq
from io import BytesIO
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery


def get_data_from_gbq(query):
    # Đường dẫn đến file JSON chứa thông tin xác thực
    # credentials = service_account.Credentials.from_service_account_file('/home/admin1/Documents/App/airflow/dw-demo-435209-d671317c69dd.json')
    credentials = service_account.Credentials.from_service_account_file('/opt/airflow/dw-demo-435209-d671317c69dd.json') #duong dan trong container
    client = bigquery.Client(credentials=credentials)

    # Thực hiện truy vấn
    query_job = client.query(query)

    # Nhận kết quả
    results = query_job.result()

    # Chuyển đổi kết quả thành DataFrame
    return results.to_dataframe()

#load_data
def load_to_gbq(info,df,schema):
    # Đường dẫn đến file JSON chứa thông tin xác thực
    # credentials = service_account.Credentials.from_service_account_file('/home/admin1/Documents/App/airflow/dw-demo-435209-d671317c69dd.json')
    credentials = service_account.Credentials.from_service_account_file('/opt/airflow/dw-demo-435209-d671317c69dd.json') #duong dan trong container
    client = bigquery.Client(credentials=credentials)

    # Khởi tạo client cho BigQuery
    bigquery_client = bigquery.Client(credentials=credentials)

    # Thông tin về bảng BigQuery
    project_id = info['project_id']
    dataset_id = info['dataset_id']
    table_id = info['table_id']
    destination_table = info['dataset_id'] + '.' + info['table_id']

    # Xóa bảng
    client.delete_table(destination_table, not_found_ok=True)

    ##check neu chua co thi tao bang moi
    dataset_ref = client.get_dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Định nghĩa schema của bảng
    schema = schema

    # Kiểm tra xem bảng đã tồn tại hay chưa
    try:
        client.get_table(table_ref)  # Nếu bảng tồn tại, không tạo bảng mới
        print(f"Bảng {table_id} đã tồn tại trong dataset {dataset_id}.")
    except:
        # Tạo bảng mới nếu chưa tồn tại
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)  # Tạo bảng
        print(f"Đã tạo bảng {table_id} trong dataset {dataset_id}.")



    #load data to gbq
    # Bước 1: Tạo tham chiếu đến bảng trong BigQuery
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    # Bước 2: Cấu hình job tải dữ liệu
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Thêm dữ liệu vào bảng hiện tại
    )

    # Bước 3: Tải DataFrame vào BigQuery
    pandas_gbq.to_gbq(
        df,
        destination_table= destination_table, 
        project_id= project_id,
        if_exists='append'
    )

    print(f"Đã tải dữ liệu vào bảng {table_id}.")



#tao summarize table   
query = """
    WITH a as (
    SELECT 
    t1.user_id,
    t3.employee_name,
    min(t1.location) check_location,
    t1.check_date,
    min(t1.check_time) as checkin_time,
    max(t1.check_time) as checkout_time,
    t2.days_week,
    t2.shift_name,
    t2.check_in_shift_time,
    t2.check_out_shift_time
    FROM `dw-demo-435209.dwh_attendance.fact_attendance` t1
    LEFT JOIN `dw-demo-435209.dwh_attendance.attendance_result` t2 on t1.user_id = t2.user_id and t1.check_date = t2.date_check
    LEFT JOIN `dw-demo-435209.dwh_attendance.dim_employee` t3 on t1.user_id = t3.user_id
    GROUP BY 
    t1.user_id, 
    t3.employee_name,
    t1.check_date,
    t2.days_week,
    t2.shift_name,
    t2.check_in_shift_time,
    t2.check_out_shift_time
    )
    SELECT 
    *,
    CASE WHEN checkin_time > check_in_shift_time THEN TIMESTAMP_DIFF(checkin_time, check_in_shift_time, MINUTE) ELSE NULL END AS so_phut_muon,
    CASE WHEN checkout_time < check_out_shift_time THEN TIMESTAMP_DIFF(check_out_shift_time, checkout_time, MINUTE) ELSE NULL END AS so_phut_ve_som,
    TIMESTAMP_DIFF(checkout_time, checkin_time, MINUTE) AS thoi_gian_lam_viec,
    TIMESTAMP_DIFF(check_out_shift_time, check_in_shift_time, MINUTE) AS thoi_gian_dinh_muc
    FROM a
"""
df = get_data_from_gbq(query)


schema_location = [
    bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("employee_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("check_location", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("check_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("checkin_time", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("checkout_time", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("days_week", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("shift_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("check_in_shift_time", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("check_out_shift_time", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("so_phut_muon", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("so_phut_ve_som", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("thoi_gian_lam_viec", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("thoi_gian_dinh_muc", "INT64", mode="NULLABLE")
]


#Thong tin google big query
#Thong tin google big query
info_gbq = {
    'project_id': 'dw-demo-435209',
    'dataset_id': 'dwh_attendance',
    'table_id': 'summarize_attendance'
}


load_to_gbq(info_gbq,df,schema_location)