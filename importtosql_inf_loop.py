import os.path
import time
import pandas as pd
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
        df = pd.read_excel(path)
        df['Początek zakłócenia'] = df['Początek zakłócenia'].astype('string')
        df['Pocz. zakłóc. (godz.)'] = df['Pocz. zakłóc. (godz.)'].astype('string')
        df['Koniec zakłóc.(godz.)'] = df['Koniec zakłóc.(godz.)'].astype('string')
        engine = create_engine(
            "mssql+pyodbc://" + u_name + ":" + u_pass + "@" + svr_name + "/" + db_name + "?driver=SQL+Server",
            fast_executemany=True)
        engine.execute("delete from MaintenanceData")
        df.to_sql('MaintenanceData', if_exists='append', con=engine, index=False)
        print('New file send to SQL!')
        last_time = time.ctime(os.path.getmtime(path))
    time.sleep(5)
    """ IF something 
            break
    """
    continue
