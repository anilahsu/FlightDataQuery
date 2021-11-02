import requests
import pandas
from sqlalchemy import create_engine
from sqlalchemy.dialects.oracle import NUMBER, NVARCHAR2, TIMESTAMP
from apscheduler.schedulers.blocking import BlockingScheduler
from auth import Auth
from db_config import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER
import json

APP_ID = '56edb16ea02b4d48a6b98eabd250e240'
APP_KEY = 'ladUqDMhhgBnvWhMuY9r4priDRY'


def fetch_flight_data():
    auth = Auth(APP_ID, APP_KEY)
    headers = auth.get_auth_header()
    response = requests.get(
        'https://ptx.transportdata.tw/MOTC/v2/Air/FIDS/Flight?format=JSON',
        headers=headers
    )
    # print(response)
    print(f"HTTP status: {response.status_code}")

    last_modified = response.headers.get("Last-Modified")
    if last_modified:
        with open('last_modified_file.txt', mode='w') as f:
            f.write(last_modified)

    
    flight_data = response.json()
    # print(flight_data)
    
    
    return flight_data


def process_data(flight_data):
    flight_df = pandas.DataFrame(flight_data)
    flight_df = flight_df.drop(['DepartureApron', 'ArrivalApron'], axis=1)
    column_type_dict = {}
    time_column_list = ['FlightDate', 'ScheduleDepartureTime', 'ActualDepartureTime', 'ScheduleArrivalTime',
                        'ActualArrivalTime', 'EstimatedArrivalTime', 'EstimatedDepartureTime', 'UpdateTime']
    

    for column, dtype in zip(flight_df.columns, flight_df.dtypes):
        if 'object' in str(dtype):
            column_type_dict.update({column: NVARCHAR2})
        if 'int' in str(dtype):
            column_type_dict.update({column: NVARCHAR2})
        if 'bool' in str(dtype):
            column_type_dict.update({column: NUMBER})
        if column in time_column_list:
            column_type_dict.update({column: TIMESTAMP})
    return flight_df, column_type_dict


def save_data_to_db(flight_df, column_type_dict):
    engine = create_engine(
        f"oracle+cx_oracle://{DB_USER}：{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    flight_df.to_sql('stock_class', con=engine,
                     index=False, if_exists='append', dtype=column_type_dict)


def get_flight_data_and_save():
    # 取得即時航班 API 資料
    flight_data = fetch_flight_data()
    if flight_data:
        # 資料處理
        flight_df, column_type_dict = process_data(flight_data)
        # 存入資料庫
        save_data_to_db(flight_df, column_type_dict)


def main():
    scheduler = BlockingScheduler()
    scheduler.add_job(get_flight_data_and_save, "interval",seconds=2)
    scheduler.start()


if __name__ == "__main__":
    main()
