import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler
import requests
from hashlib import sha1
import hmac
from wsgiref.handlers import format_date_time
from datetime import datetime
from time import mktime
import time
import base64
import requests
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler


# 取得 API 資料
app_id = '56edb16ea02b4d48a6b98eabd250e240'
app_key = 'ladUqDMhhgBnvWhMuY9r4priDRY'


class Auth():

    def __init__(self, app_id, app_key):
        self.app_id = app_id
        self.app_key = app_key

    def get_auth_header(self):
        xdate = format_date_time(mktime(datetime.now().timetuple()))
        hashed = hmac.new(self.app_key.encode('utf8'),
                          ('x-date: ' + xdate).encode('utf8'), sha1)
        signature = base64.b64encode(hashed.digest()).decode()

        authorization = 'hmac username="' + self.app_id + '", ' + \
                        'algorithm="hmac-sha1", ' + \
                        'headers="x-date", ' + \
                        'signature="' + signature + '"'

        # 將 if-modified-since 寫入 function
        try:
            with open('file_last_modified.txt', mode='r', encoding='utf-8') as f:
                last_time = f.read()
        except:
            last_time = None

        return {
            'Authorization': authorization,
            'x-date': format_date_time(mktime(datetime.now().timetuple())),
            'Accept - Encoding': 'gzip',
            'If-Modified-Since': last_time
        }


if __name__ == '__main__':
    auth = Auth(app_id, app_key)
    headers = auth.get_auth_header()

    print('header', headers)

    response = requests.get(
        'https://ptx.transportdata.tw/MOTC/v2/Air/FIDS/Flight?format=JSON', headers=auth.get_auth_header())

    print(f"HTTP status: {response.status_code}")

    # 新增 if-modified-Since
    last_modified_time = response.headers.get('Last-Modified')

    print(last_modified_time)
    if last_modified_time:
        with open('file_last_modified.txt', mode='w') as f:
            f.write(last_modified_time)



# def dojob():
#     scheduler = BlockingScheduler()
#     scheduler.add_job(fun1, 'interval', seconds=2)
#     scheduler.start()

# dojob()
