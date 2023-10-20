import os.path
import time
import pandas as pd
from sqlalchemy import create_engine
import logging
import win32com.client

configfile = pd.read_csv('config_nexeed_xls.txt', header=None).reset_index()

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 12)

logging.basicConfig(filename='NEXEED_TO_SQL_LOGS.txt', filemode='a', level=logging.INFO,
                    format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

# Server
svr_name = 'LDWSWISQLVAL01'
db_name = 'NexeedReport'
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
                    xlApp = win32com.client.Dispatch("Excel.Application")
                    xlWbk = xlApp.Workbooks.Open(row[0])
                    xlApp.DisplayAlerts = False
                    xlWbk.SaveAs(row[1], 51)
                    xlApp.DisplayAlerts = True
                    xlWbk.Close(True)
                    xlApp.Quit()
                except Exception as e:
                    logger.error(e)
                    last_time[index] = 0
                finally:
                    xlWbk = None
                    xlApp = None
                    del xlWbk
                    del xlApp
                try:
                    output_df = pd.read_excel(row[1])
                    output_df.to_sql(row[2], if_exists='replace', con=engine, index=False)
                    last_time[index] = time.ctime(os.path.getmtime(row[0]))
                    logger.info(f'New file "{row[0]}" send to SQL table "{row[2]}"!')
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
