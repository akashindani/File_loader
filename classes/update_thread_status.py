import cx_Oracle
from classes.conn_details import conn_details

class update_thread_status(conn_details):
    
    def __init__(self):
       super(update_thread_status,self).__init__()
    
    def set_thread_status(self,status,thread_name):
        
        update_thr_status="update thread_control set thread_status='DOWN' where lower(thread_name)=lower('"+thread_name+"')"    
        
        try:
            con = cx_Oracle.connect(self.app_db_conn_str)
            cur = con.cursor()
            
            try:
                print(update_thr_status)
                cur.prepare(update_thr_status)
                cur.execute(None)
                con.commit()
            
            except Exception as e:
                print(str(e))
                print("caused by : Cannot run query :")
            
            finally:
                cur.close()
                con.close()
                
        
        except Exception as e:
            print(str(e))
            print("caused by : Cannot connect ot oracle ")
            
       
    
