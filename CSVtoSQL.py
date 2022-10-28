import os.path
import time
import pandas as pd
from pandas.io.parsers import read_csv
from sqlalchemy import create_engine

with open('config.txt') as f:
    path = f.read()
    print(path)

# Sec
svr_name = 'LDWSWISQLVAL01'
db_name = 'MaintenanceReport'
u_name = 'PowerBI'
u_pass = '111098tdg'

last_time = time.ctime(os.path.getmtime(path))


while True:
    if time.ctime(os.path.getmtime(path)) != last_time:
        rawCSV = pd.read_csv(path, sep=';')

        rawCSV.drop('Unnamed: 0', inplace=True, axis=1)
        rawCSV.drop('Unnamed: 13', inplace=True, axis=1)

        rawCSV = rawCSV.loc[(rawCSV["  Pocz.zakł."] != "  Pocz.zakł.")]

        engine = create_engine("mssql+pyodbc://" + u_name + ":" + u_pass + "@" + svr_name + "/" + db_name +
                               "?driver=ODBC Driver 11 for SQL Server", fast_executemany=True)
        engine.execute("delete from MaintenanceData")
        rawCSV.to_sql('MaintenanceData', if_exists='append', con=engine, index=False)
        print('New file send to SQL!')
        last_time = time.ctime(os.path.getmtime(path))
    time.sleep(5)
    """ IF something 
            break
    """
    continue
