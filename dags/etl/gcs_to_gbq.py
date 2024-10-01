import os
import pandas as pd
import pandas_gbq
from io import BytesIO
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery


def extract_from_gcs(path_file):

    # Đặt đường dẫn tới tệp credentials thông qua biến môi trường
    # credentials = service_account.Credentials.from_service_account_file('/home/admin1/Documents/App/airflow/dw-demo-435209-d671317c69dd.json')
    credentials = service_account.Credentials.from_service_account_file(os.getenv('AIRFLOW_GOOGLE_CREDS')) #duong dan trong container
    client = storage.Client(credentials=credentials)

    # Tên bucket và tệp CSV trên GCS
    bucket_name = path_file['bucket_name']
    csv_file_path = path_file['path']

    # Lấy bucket và blob
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(csv_file_path)

    # Tải tệp CSV về
    csv_data = blob.download_as_bytes()

    # Đọc tệp CSV vào pandas DataFrame
    result = pd.read_csv(BytesIO(csv_data))

    return result


#tranform dim_employee
def transform_employee(df_employee):

    #rename column
    result = df_employee.rename(columns={
        'User_id': 'user_id',
        'Date Created': 'created_date',
        'Last Modified Date': 'modified_date',
        'Departments': 'department',
        'Email': 'email',
        'Employee_type': 'employee_type',
        'Join_time': 'join_time',
        'Mobile': 'mobile',
        'User Name': 'employee_name',
        'Gender': 'gender'
    })

    #transform type
    result['created_date'] = pd.to_datetime(result['created_date'], unit='ms', errors='coerce')
    result['modified_date'] = pd.to_datetime(result['modified_date'], unit='ms', errors='coerce')
    result['join_time'] = pd.to_datetime(result['join_time'],unit='ms', errors='coerce')
    result['mobile'] = result['mobile'].apply(lambda x: str(x).replace('.0', ''))

    return result


#tranform location
def transform_location(df_location):

    df = df_location[['Check location name','Is offsite']].drop_duplicates(['Check location name','Is offsite'])
    df = df.reset_index(drop=True)


    #rename column
    result = df.rename(columns={
        'Check location name': 'location',
        'Is offsite': 'is_offsite',
    })

    #add column created and modified
    result['created_date'] = pd.Timestamp.now().floor('min')
    result['modified_date'] = pd.Timestamp.now().floor('min')

    return result


#transform attendance
def transform_attendance(df):

    df_attendance = df.rename(columns={
        'Record id': 'record_id',
        'Employee': 'employee',
        'Check location name': 'location',
        'Check time': 'check_time',
        'Date': 'check_date',
    })

    #lay thong tin dim_employee
    # Câu truy vấn SQL
    query = """
        SELECT *
        FROM dw-demo-435209.dwh_attendance.dim_employee
    """

    gbq_employee = get_data_from_gbq(query)


    #join voi bang chinh lay user_id
    result = pd.merge(df_attendance, gbq_employee, left_on='employee', right_on='employee_name', how='left')
    df_attendance_final = result[['record_id','user_id','location','check_date','check_time']]
    df_attendance_final = df_attendance_final.dropna(subset=['record_id','user_id','check_time'])

    #transform type
    df_attendance_final['check_date'] = pd.to_datetime(df_attendance_final['check_date'], unit='ms', errors='coerce') + pd.to_timedelta(7, unit='h')
    df_attendance_final['check_date'] = df_attendance_final['check_date'].dt.date
    df_attendance_final['check_time'] = pd.to_datetime(df_attendance_final['check_time'], unit='ms', errors='coerce') + pd.to_timedelta(7, unit='h')
    df_attendance_final['created_date'] = pd.Timestamp.now().floor('min')
    df_attendance_final['modified_date'] = pd.Timestamp.now().floor('min')

    return df_attendance_final


#transform attendance result
def transform_attendance_result(df):

    df_attendance_result = df.rename(columns={
        'Result id' : 'result_id',
        'User id' : 'user_id',
        'Date' : 'date_check',
        'Days_week' : 'days_week',
        'Shift name' : 'shift_name',
        'Check in shift time' : 'check_in_shift_time',
        'Check out shift time' : 'check_out_shift_time'
    })

    #transform type
    df_attendance_result['date_check'] = pd.to_datetime(df_attendance_result['date_check'], unit='ms') + pd.to_timedelta(7, unit='h')
    df_attendance_result['check_in_shift_time'] = pd.to_datetime(df_attendance_result['check_in_shift_time'], unit='ms', errors='coerce') + pd.to_timedelta(7, unit='h')
    df_attendance_result['check_out_shift_time'] = pd.to_datetime(df_attendance_result['check_out_shift_time'], unit='ms', errors='coerce') + pd.to_timedelta(7, unit='h')
    df_attendance_result['created_date'] = pd.Timestamp.now().floor('min')
    df_attendance_result['modified_date'] = pd.Timestamp.now().floor('min')

    #drop na
    result = df_attendance_result.dropna(subset=['result_id','user_id','date_check'])

    return result


#load_data
def load_to_gbq(info,df,schema):
    # Đường dẫn đến file JSON chứa thông tin xác thực
    # credentials = service_account.Credentials.from_service_account_file('/home/admin1/Documents/App/airflow/dw-demo-435209-d671317c69dd.json')
    credentials = service_account.Credentials.from_service_account_file(os.getenv('AIRFLOW_GOOGLE_CREDS')) #duong dan trong container
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

#get data from gbq
def get_data_from_gbq(query):
    # Đường dẫn đến file JSON chứa thông tin xác thực
    # credentials = service_account.Credentials.from_service_account_file('/home/admin1/Documents/App/airflow/dw-demo-435209-d671317c69dd.json')
    credentials = service_account.Credentials.from_service_account_file(os.getenv('AIRFLOW_GOOGLE_CREDS')) #duong dan trong container
    client = bigquery.Client(credentials=credentials)

    # Thực hiện truy vấn
    query_job = client.query(query)

    # Nhận kết quả
    results = query_job.result()

    # Chuyển đổi kết quả thành DataFrame
    return results.to_dataframe()


##
##
def task2_gcs_to_gbq():
    #ETL_dim_employee
    path_gcs={
        'bucket_name': 'attendance_data2024',
        'path': 'employee_info.csv'
    }

    schema_employee = [
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("employee_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("employee_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mobile", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("department", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("modified_date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("join_time", "TIMESTAMP", mode="REQUIRED"),
    ]

    df_employee = extract_from_gcs(path_gcs)
    df_employee_final = transform_employee(df_employee)

    #Thong tin google big query
    info_gbq = {
        'project_id': 'dw-demo-435209',
        'dataset_id': 'dwh_attendance',
        'table_id': 'dim_employee'
    }

    load_to_gbq(info_gbq,df_employee_final,schema_employee)



    #ETL dim_location
    path_gcs={
        'bucket_name': 'attendance_data2024',
        'path': 'attendance_record.csv'
    }

    df_location = extract_from_gcs(path_gcs)
    df_location_final = transform_location(df_location)


    schema_location = [
        bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_offsite", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("created_date", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("modified_date", "DATETIME", mode="REQUIRED")
    ]


    #Thong tin google big query
    info_gbq['table_id'] = 'dim_location'

    load_to_gbq(info_gbq,df_location_final,schema_location)


    ##ETL fact_attendance
    path_gcs={
        'bucket_name': 'attendance_data2024',
        'path': 'attendance_record.csv'
    }

    #trich xuat du lieu attendance
    df_attendance = extract_from_gcs(path_gcs)
    df_attendance_final = transform_attendance(df_attendance)


    info_gbq = {
        'project_id': 'dw-demo-435209',
        'dataset_id': 'dwh_attendance',
        'table_id': 'fact_attendance'
    }

    schema_attendance = [
        bigquery.SchemaField("record_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("check_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("check_time", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("created_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("modified_date", "DATETIME", mode="NULLABLE")
    ]

    load_to_gbq(info_gbq,df_attendance_final,schema_attendance)



    ##ETL attendance_result
    path_gcs={
        'bucket_name': 'attendance_data2024',
        'path': 'attendance_result.csv'
    }

    #trich xuat du lieu attendance
    df_attendance_result = extract_from_gcs(path_gcs)
    df_attendance_result_final = transform_attendance_result(df_attendance_result)


    info_gbq = {
        'project_id': 'dw-demo-435209',
        'dataset_id': 'dwh_attendance',
        'table_id': 'attendance_result'
    }

    schema_attendance = [
        bigquery.SchemaField("result_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date_check", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("days_week", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("shift_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("check_in_shift_time", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("check_out_shift_time", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("created_date", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("modified_date", "DATETIME", mode="NULLABLE")
    ]

    load_to_gbq(info_gbq,df_attendance_result_final,schema_attendance)