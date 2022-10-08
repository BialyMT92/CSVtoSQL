import shutil

import pandas as pd
from sqlalchemy import create_engine

#Sec
svr_name = 'LDWSWISQLVAL01'
db_name = 'MaintenanceReport'
u_name = 'PowerBI'
u_pass = '111098tdg'

df = pd.read_excel(r"\\ldwna001\Data\Factory Lodz\Departments\FDCL-PD\FDCL-PDT\7. MYSZKA KATARZYNA\05. PROJEKTY\Wizualizacja PowerBI\Awarie - dane STOEWA.xlsx")

df['Początek zakłócenia'] = df['Początek zakłócenia'].astype('string')
df['Pocz. zakłóc. (godz.)'] = df['Pocz. zakłóc. (godz.)'].astype('string')
df['Koniec zakłóc.(godz.)'] = df['Koniec zakłóc.(godz.)'].astype('string')

engine = create_engine("mssql+pyodbc://"+u_name+":"+u_pass+"@"+svr_name+"/MaintenanceReport?driver=SQL+Server", fast_executemany=True)
engine.execute("delete from MaintenanceData")

df.to_sql('MaintenanceData', if_exists='append', con=engine, index=False, chunksize=100000)

