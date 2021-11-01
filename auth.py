import base64
from datetime import datetime
from hashlib import sha1
import hmac
from time import mktime
from wsgiref.handlers import format_date_time


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
            with open('last_modified_file.txt', mode='r', encoding='utf-8') as f:
                last_time = f.read()
        except:
                last_time = None

        return {
            'Authorization': authorization,
            'x-date': format_date_time(mktime(datetime.now().timetuple())),
            'Accept - Encoding': 'gzip',
            'If-Modified-Since': last_time
        }


if __name__ == "__main__":
    pass
