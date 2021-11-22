

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
    data = [
        {
        # "FlightDate": "2021-11-09",
        "name": "mimi",
        "airportname":"QtttttQQ",
        "flightdate":"2021-12-11"
        # "airroutetype": "333333",
        # "airlineid": "DdddddddddA",
        # "DepartureAirportID": "CMJ",
        # "ArrivalAirportID": "MZG",
        # "ScheduleDepartureTime": "2021-11-09T12:00",
        # "ActualDepartureTime": "2021-11-09T11:58",
        # "ScheduleArrivalTime": "2021-11-09T12:20",
        # "ActualArrivalTime": "2021-11-09T12:15",
        # "DepartureRemark": "已飛",
        # "ArrivalRemark": "抵達",
        # "ArrivalTerminal": "",
        # "DepartureTerminal": "",
        # "ArrivalGate": "",
        # "DepartureGate": "",
        # "IsCargo": "false",
        # "UpdateTime": "2021-11-09T18:42:31+08:00"
        }
    ]
    return data

def process_data(data):
    print(data)
    df = pandas.DataFrame(data)
    
    for i in range(0,len(df)):
        print('for i in df:',df.values)

    # flight_df = df.drop(['DepartureApron', 'ArrivalApron'], axis=1)

    column_type_dict = {}
    time_column_list = ['flightdate']
    #  ['FlightDate', 'ScheduleDepartureTime', 'ActualDepartureTime', 'ScheduleArrivalTime',
    #                     'ActualArrivalTime', 'EstimatedArrivalTime', 'EstimatedDepartureTime', 'UpdateTime']
    
    # 將 fligjt_df 中的時間欄位從 object 轉成 datatime 
    # 使 python (識別)類別對應至 sql 類別                
    for column in df.columns:
        if (column in time_column_list):
            df[column] = pandas.to_datetime(df[column])

    for column, dtype in zip(df.columns, df.dtypes):
        if 'object' in str(dtype):
            column_type_dict.update({column: NVARCHAR2})
        # if 'int' in str(dtype):
        #     column_type_dict.update({column: NVARCHAR2})
        # if 'bool' in str(dtype):
        #     column_type_dict.update({column: NUMBER})
        if column in time_column_list:
            column_type_dict.update({column: TIMESTAMP})
    return df, column_type_dict


def save_data_to_db(df, column_type_dict):
    engine = create_engine(f"oracle+cx_oracle://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    df.to_sql('TESTTABLE', con=engine,
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
    get_flight_data_and_save()


if __name__ == "__main__":
    main()
