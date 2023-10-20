import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import os.path
import time
import pandas as pd
from sqlalchemy import create_engine
import logging
import tabula


class Pythonservice(win32serviceutil.ServiceFramework):
    _svc_name_ = 'NexeedPDF_OEE_DataToSQL'
    _svc_display_name_ = 'NexeedPDF_OEE_DataToSQL'
    _svc_description_ = 'Sending Nexeed OEE PDF File to SQL when modification time of the file changed'

    @classmethod
    def parse_command_line(cls):
        win32serviceutil.HandleCommandLine(cls)

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.stop()
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        self.start()
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def start(self):
        self.isrunning = True

    def stop(self):
        self.isrunning = False

    def main(self):

        configfile = pd.read_csv('D:\CopyToSQL\config_nexeed_pdf_oee.txt', header=None).reset_index()

        logging.basicConfig(filename='D:\CopyToSQL\NEXEED_PDF_OEE_TO_SQL_LOGS.txt', filemode='a', level=logging.INFO,
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
                            NexeedDF.columns = ['Shift Begin', 'Shift End', 'Availability [%]', 'Efficiency [%]',
                                                'Quality [%]',
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
            if win32event.WaitForSingleObject(self.hWaitStop, 5000) == win32event.WAIT_OBJECT_0:
                break
            continue


if __name__ == '__main__':
    Pythonservice.parse_command_line()
