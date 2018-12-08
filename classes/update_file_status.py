import cx_Oracle
from classes.conn_details import conn_details

class update_file_status(conn_details):
    
    def __init__(self):
       super(update_file_status,self).__init__()
    
    def update_file_status(self,status,identifier,error_desc='None',table_name='None'):
        
        update_file_process="update file_control set file_status='"+status+"',sys_update_date=sysdate,error_desc='"+error_desc+"',temp_table_name='"+table_name+"' where identifier='"+identifier+"'"    
        
        try:
            con = cx_Oracle.connect(self.app_db_conn_str)
            cur = con.cursor()
            
            try:
                print(update_file_process)
                cur.prepare(update_file_process)
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
            
       
    
