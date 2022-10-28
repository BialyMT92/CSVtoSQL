import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import os.path
import time
import pandas as pd
from sqlalchemy import create_engine


class Pythonservice(win32serviceutil.ServiceFramework):
    _svc_name_ = 'CSVDataToSQL'
    _svc_display_name_ = 'CSVDataToSQL'
    _svc_description_ = 'Sending CSV File to SQL when modification time of the file changed'

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

        with open('D:\CopyToSQL\config.txt') as f:
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
            if win32event.WaitForSingleObject(self.hWaitStop, 5000) == win32event.WAIT_OBJECT_0:
                break
            continue


if __name__ == '__main__':
    Pythonservice.parse_command_line()
