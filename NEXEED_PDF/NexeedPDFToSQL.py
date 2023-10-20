import os.path
import time
import pandas as pd
from sqlalchemy import create_engine
import logging
import win32com.client
import tabula


configfile = pd.read_csv('config_nexeed_pdf_oee.txt', header=None).reset_index()

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 12)

logging.basicConfig(filename='EXEED_PDF_OEE_TO_SQL_LOGS.txt', filemode='a', level=logging.INFO,
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
                    NexeedList = []

                    NexeedDF = tabula.read_pdf(row[0], pages='all')
                    for item in NexeedDF:
                        for info in item.values:
                            list_no_nan = [x for x in info if pd.notnull(x)]
                            NexeedList.append(list_no_nan)
                    NexeedDF = pd.DataFrame(NexeedList)
                    NexeedDF = NexeedDF.drop(NexeedDF.index[0:5])
                    NexeedDF = NexeedDF[NexeedDF[1] != '+02:00']
                    NexeedDF = NexeedDF.reset_index(drop=True)
                    NexeedDF.columns = ['Shift Begin', 'Shift End', 'Availability [%]', 'Efficiency [%]', 'Quality [%]',
                                        'OEE [%]', 'Equipment']
                    NexeedDF['Shift Begin'] = NexeedDF['Shift Begin'].str.split('\r').str[0]
                    NexeedDF['Shift End'] = NexeedDF['Shift End'].str.split('\r').str[0]

                    NexeedDF.to_sql(row[1], if_exists='replace', con=engine, index=False)
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




