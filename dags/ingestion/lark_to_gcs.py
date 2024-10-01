import requests
import json
import pandas as pd
import io
import os
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime


def get_data_larkbase(token):

    #lay tenant_access_token
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = json.dumps({
        "app_id": token['app_id'],
        "app_secret": token['app_secret']
    })

    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request('POST',url, headers=headers, data=payload)

    if response.status_code == 200:
        info = json.loads(response.text)
        tenant_access_token ='Bearer ' + info['tenant_access_token']

    else:
        print(f"Error: {response.status_code} - {response.text}")


    #lay data tu bang Lark Attendance Result can tenant_access_token, app_token, table_id
    app_token = token['app_token']
    table_id = token['table_id']
    page_token = ''
    payload = ''

    headers = {
    'Authorization': tenant_access_token
    }

    records = []

    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records?page_size=500&page_token={page_token}"

    response = requests.request("GET", url, headers=headers, data=payload)

    data = json.loads(response.text)
    records.extend(data['data']['items'])

    #lay trang thai has_more
    has_more = data['data']['has_more']


    #1 api lay toi da 500 record, phai loop qua tung trang de lay het data (neu co thi has_more = True)
    while has_more == True:
        page_token = data['data']['page_token']
        url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records?page_size=500&page_token={page_token}"

        response = requests.request("GET", url, headers=headers, data=payload)

        data = json.loads(response.text)
        records.extend(data['data']['items'])

        has_more = data['data']['has_more']

    result = [record['fields'] for record in records]

    return result


def lark_to_gcs(file_load,df):

    # Sử dụng io.StringIO để ghi DataFrame vào bộ nhớ dưới dạng CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, float_format='%.0f')

    # Khởi tạo client Google Cloud Storage
    # credentials = service_account.Credentials.from_service_account_file('/home/admin1/Documents/App/airflow/dw-demo-435209-d671317c69dd.json') #duong dan local
    credentials = service_account.Credentials.from_service_account_file(os.getenv('AIRFLOW_GOOGLE_CREDS')) #duong dan trong container
    client = storage.Client(credentials=credentials)


    # Lấy bucket từ tên bucket đã tạo trên GCS
    bucket = client.get_bucket(file_load['bucket'])

    # Tạo blob (file) để upload
    blob = bucket.blob(file_load['path'])

    # Upload CSV lên blob
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')



def task1_lark_to_gcs():
    #khai bao token
    token = {
        "app_id": "cli_a66321e17db81010",
        "app_secret": "38ub3IgwcaZB6wpLNVzw4gkYb0Z72Qe3",
        'app_token': 'U0PMbECFHat1KDsd3Bol6qRKg5g'
    }

    #Employee_info table
    #get data_from_lark
    token['table_id'] = 'tblfnsvvcioeTn42'

    data_employee = get_data_larkbase(token)

    df_employee = pd.DataFrame({
        'User_id' : [record.get('user_id','') for record in data_employee],
        'Date Created': [record.get('Date Created', '') for record in data_employee],
        'Last Modified Date': [record.get('Last Modified Date', '') for record in data_employee],
        'Departments': [record.get('departments', [{}])[0].get('text', '') for record in data_employee],
        'Email': [record.get('email', '') for record in data_employee],
        'Employee_type': [record.get('employee_type','') for record in data_employee],
        'Join_time' : [record.get('join_time','') for record in data_employee],
        'Mobile': [record.get('mobile', '') for record in data_employee],
        'User Name': [record['user'][0].get('name', '') if 'user' in record and record['user'] else '' for record in data_employee],
        'Gender': [record.get('gender','') for record in data_employee]
    })


    #load data from to gcs
    file_load ={
        'bucket': 'attendance_data2024',
        'path': 'employee_info.csv'
    }

    load_data = lark_to_gcs(file_load,df_employee)


    #Attendance_record table
    #get data_from_lark
    token['table_id'] = 'tbl2o2C3y6HtDBr5'

    data_attendance = get_data_larkbase(token)


    df_attendance = pd.DataFrame({
        'Record id': [record.get('Record id', '') for record in data_attendance],
        'Check location name': [record.get('Check location name', '') for record in data_attendance],
        'Check time': [record.get('Check time', '') for record in data_attendance],
        'Date': [record.get('Date','') for record in data_attendance],
        'Date Created': [record.get('Date Created', '') for record in data_attendance],
        'Employee': [record['Employee'][0].get('name','') for record in data_attendance],
        'Is offsite' : [record.get('Is offsite','') for record in data_attendance],
        'Last Modified Date': [record.get('Last Modified Date', '') for record in data_attendance],
        'Month' : [record['Tháng'][0].get('text','') for record in data_attendance]
    })


    #load data from to gcs
    file_load ={
        'bucket': 'attendance_data2024',
        'path': 'attendance_record.csv'
    }

    load_data = lark_to_gcs(file_load,df_attendance)



    #Attendance_result table
    #get data_from_lark
    token['table_id'] = 'tblzPogj1pOD3LZN'

    data_attendance_result = get_data_larkbase(token)

    df_attendance_result = pd.DataFrame({
        'Result id': [record.get('Result id', '') for record in data_attendance_result],
        'User id': [record.get('User id', '') for record in data_attendance_result],
        'Date': [record.get('Date', '') for record in data_attendance_result],
        'Days_week': [record.get('Days_week','') for record in data_attendance_result],
        'Shift name': [record.get('Shift name', '') for record in data_attendance_result],
        'Check in shift time': [record.get('Check in shift time','') for record in data_attendance_result],
        'Check out shift time' : [record.get('Check out shift time','') for record in data_attendance_result]
    })

    #load data from to gcs
    file_load ={
        'bucket': 'attendance_data2024',
        'path': 'attendance_result.csv'
    }

    load_data = lark_to_gcs(file_load,df_attendance_result)
