import os.path
import time
import pandas as pd
from sqlalchemy import create_engine
import logging

configfile = pd.read_csv('C:/Users/Public/Documents/GIT/python_excel_to_sql/CSV/config_csv.txt', header=None).reset_index()

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 12)
pd.set_option('display.max_rows', None)

logging.basicConfig(filename='C:/Users/Public/Documents/GIT/python_excel_to_sql/CSV/CSV_TO_SQL_LOGS.txt', filemode='a', level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

# Server
svr_name = 'LDWSWISQLVAL01'
db_name = 'MaintenanceReport'
u_name = 'PowerBI'
u_pass = '111098tdg'

engine = create_engine("mssql+pyodbc://" + u_name + ":" + u_pass + "@" + svr_name + "/" + db_name +
                       "?driver=ODBC Driver 11 for SQL Server", fast_executemany=True)

last_time = [0] * len(configfile)
while True:
    for index, row in configfile.iterrows():
        try:      
            if time.ctime(os.path.getmtime(row[0])) != last_time[index]:
                try:
                    with open(row[0], 'r', encoding='utf-8') as f:
                        lines = f.readlines()

                    skip = 0
                    for i, line in enumerate(lines):
                        if line.count(';') >= 1:  # first row with semicolon-separated values
                            break
                        else:
                            skip = i+1
                    rawCSV = pd.read_csv(row[0], sep=';', skiprows=skip, on_bad_lines='skip')
                    rawCSV.drop(list(rawCSV.filter(regex='Unnamed')), inplace=True, axis=1)
                    rawCSV = rawCSV[(rawCSV != rawCSV.columns).all(axis=1)]
                    rawCSV.columns = rawCSV.columns.str.lower()
                    rawCSV.columns = rawCSV.columns.str.strip()
                    cols = pd.Series(rawCSV.columns)
                    for dup in cols[cols.duplicated()].unique():
                        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in
                                                                         range(sum(cols == dup))]
                    rawCSV = rawCSV.dropna(thresh=3)
                    rawCSV.columns = cols
                    rawCSV.to_sql(row[1], if_exists='replace', con=engine, index=False)
                    last_time[index] = time.ctime(os.path.getmtime(row[0]))
                    logger.info(f'New file "{row[0]}" send to SQL table "{row[1]}"!')
                except:
                    logger.error(f'Service was not able to send "{row[0]}" to SQL! Problem with the file.')
                    last_time[index] = 0
        except:
            logger.error(f'File "{row[0]} not found!')
            last_time[index] = 0
        

    time.sleep(10)
    """ IF something 
            break
    """
    continue
