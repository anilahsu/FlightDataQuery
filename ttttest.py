import pandas as pd
from db_config import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER
import cx_Oracle
from sqlalchemy import create_engine

# import numpy as np


lst1 = ["Jay","Raj","Jack"]
lst2 = [12,15,14]
df = pd.DataFrame(list(zip(lst1,lst2)), columns = ['Name','Age'])
# print(df)
df.columns = df.columns.map(lambda x:x.lower())
print(df)
print(len(df))

# keys = ["tw", "us", "it"]
# values = [2.1, 3.8, 6.9]
# dict_test = dict(zip(keys, values))
# print(dict_test)

# x='AEFVdde'
# y = x.lower()

# print(df['name'][[0]])
list_drop = [0,1]
str_index = str(list_drop)[1:-1]
index =map(int,str_index) 
# print(index)

df2=df.drop(index=list_drop)
# print(*list_drop, sep = ',') 


# print(df2)
table_list=['name']
# print(df.columns)

for i in df.columns:
    if i not in table_list:
        df3 = df.drop([f'{i}'],axis=1)

# print(df3)
# engine = create_engine(f"oracle+cx_oracle://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
# db = cx_Oracle.connect(f"{DB_USER}/{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
# cur = db.cursor()
    
# for i in range(0,len(df)):
#     exists = cur.execute("select * from flight where flightnumber = :flightnumber and airlineid = :airlineid and departureairportid = :departureairportid and arrivalairportid = :arrivalairportid and schedulearrivaltime = :schedulearrivaltime",flightnumber=new_flight_df['flightnumber'][i],airlineid=new_flight_df['airlineid'][i],departureairportid=new_flight_df['departureairportid'][i],arrivalairportid=new_flight_df['arrivalairportid'][i],schedulearrivaltime=new_flight_df['schedulearrivaltime'][i] )
#     if exists:
#     	flight_df_distinct = df.drop(df.index[i])
#         print(flight_df_distinct)
            
# cur.close()
# db.commit()
# flight_df_distinct.to_sql('flight', con=engine,
#     index=False, if_exists='append')

for index,row in df.copy().iterrows():
    print(index)
