import cx_Oracle
from db_config import *

db = cx_Oracle.connect(f"{DB_USER}/{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
cur = db.cursor()
cur.execute("SELECT * FROM FLIGHT")
records = cur.fetchall()
print(records)
cur.close()