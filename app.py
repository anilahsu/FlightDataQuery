import requests
import pandas
from sqlalchemy import create_engine
from sqlalchemy.dialects.oracle import NUMBER, NVARCHAR2, TIMESTAMP
from apscheduler.schedulers.blocking import BlockingScheduler
from auth import Auth
from db_config import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER
import json
import cx_Oracle

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
    # print('response.text:',response.text)
    if response.status_code != 304:
        flight_data = json.loads(response.text.encode(encoding='utf-8'))
        # print('flight_data:',flight_data)
        return flight_data


def process_data(flight_data):
    flight_df = pandas.DataFrame(flight_data)
    print('all_count:', len(flight_df))
    flight_df.columns = flight_df.columns.map(lambda x: x.lower())

    table_list = ['flightdate', 'flightnumber', 'airroutetype', 'airlineid', 'departureairportid', 'arrivalairportid', 'scheduledeparturetime',
                  'actualdeparturetime', 'schedulearrivaltime', 'actualarrivaltime', 'departureremark', 'arrivalremark', 'arrivalterminal', 'departureterminal',
                  'arrivalgate', 'departuregate', 'iscargo', 'updatetime', 'estimatedarrivaltime', 'estimateddeparturetime', 'checkcounter', 'baggageclaim']
    drop_list = []
    for column in flight_df.columns:
        # print(column)
        if column not in table_list:
            # print('not in',column)
            drop_list.append(column)
    new_flight_df = flight_df.drop(drop_list, axis=1)

    # print(new_flight_df.info())

    # flight_df['hash'] = flight_df[['flightnumber','airlineid','departureairportid','arrivalairportid','schedulearrivaltime']].apply(' '.join, axis=1)
    # flight_df['hash'] = flight_df["flightnumber"] + " " + flight_df["airlineid"] + " " + flight_df["departureairportid"] + " " + flight_df["arrivalairportid"] + " " + flight_df["schedulearrivaltime"]
    # print(flight_df['hash'])
    # flight_df['hash'] = flight_df['hash'].apply(hash)
    # print(new_flight_df.info())

    column_type_dict = {}
    time_column_list = ['flightdate', 'scheduledeparturetime', 'actualdeparturetime', 'schedulearrivaltime',
                        'actualarrivaltime', 'estimatedarrivaltime', 'estimateddeparturetime', 'updatetime']


    # 將 fligjt_df 中的時間欄位從 object 轉成 datatime
    # 使 python (識別)類別對應至 sql 類別
    for column in new_flight_df.columns:
        if (column in time_column_list):
            new_flight_df[column] = pandas.to_datetime(new_flight_df[column])

    for column, dtype in zip(new_flight_df.columns, new_flight_df.dtypes):
        if 'object' in str(dtype):
            column_type_dict.update({column: NVARCHAR2(60)})
        if 'int' in str(dtype):
            column_type_dict.update({column: NVARCHAR2(60)})
        if 'bool' in str(dtype):
            column_type_dict.update({column: NVARCHAR2(60)})
        if column in time_column_list:
            column_type_dict.update({column: TIMESTAMP})
    return new_flight_df, column_type_dict


def save_data_to_db(new_flight_df, column_type_dict):

    engine = create_engine(
        f"oracle+cx_oracle://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    # new_flight_df.to_csv('test.csv')

    db = cx_Oracle.connect(f"{DB_USER}/{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    cur = db.cursor()

    drop_count = 0
    for index, row in new_flight_df.copy().iterrows():
        cur.execute('''select * from flight 
            where flightnumber = :flightnumber 
                and airlineid = :airlineid 
                and departureairportid = :departureairportid
                and arrivalairportid = :arrivalairportid
                and flightdate = :flightdate''',
                    flightnumber=row['flightnumber'],
                    airlineid=row['airlineid'],
                    departureairportid=row['departureairportid'],
                    arrivalairportid=row['arrivalairportid'],
                    flightdate=row['flightdate'])
        
        exists = cur.fetchone()

        if exists:

            new_flight_df.drop(index=index,inplace=True)
            drop_count+=1
        else:
            print(row)

    print('drop_count:', drop_count)
    # print(new_flight_df.info())
    print('new_flight_df_count:', len(new_flight_df))

    cur.close()
    db.commit()

    new_flight_df.to_sql('flight', con=engine,
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
