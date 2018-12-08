import cx_Oracle
from classes.conn_details import conn_details
from classes.load_csv import load_csv
from classes.update_thread_status import update_thread_status
import time

class load_csv_thread(conn_details):
    
    def __init__(self):
       super(load_csv_thread,self).__init__()
    
    def trigger_thread(self):
        
        check_thread_status="select * from thread_control where lower(Thread_name)='load_csv_thread' and lower(thread_status)='up'"
        
        try:
            con = cx_Oracle.connect(self.app_db_conn_str)
            cur = con.cursor()
            cur.prepare(check_thread_status)
      
            while cur.execute(None) and cur.fetchall():
                #for row in cur.fetchall():
                #print (str(row[0]).strip())
                print ("Started checking for loading new csv")
                load_csv_object=load_csv()
                load_csv_object.csv_loader()
                time.sleep(30)
            
            print("Going to end the load_csv as the thread status is down in thread_control")
            
        except Exception as e:
            print ("Exception came in load_csv")
            print (str(e))
            update_thread_status_object=update_thread_status()
            update_thread_status_object.set_thread_status('DOWN','load_csv_thread')