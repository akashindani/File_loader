from classes.update_file_status import update_file_status
from classes.conn_details import conn_details

class run_csv_sqlldr(conn_details):
    
    def __init__(self):
       super(run_csv_sqlldr,self).__init__()
    
    def run_sqlldr(self,qa_file,qa_log,identifier,temp_table,ssh_client):
        try:    
            sql_loader_command="""cd """+str(self.temp_dir)+""" ;
            sqlldr userid="""+self.sqlloader_db_con_str+" control="+str(qa_file)+" log="+str(qa_log)
            print (sql_loader_command)
            #stdin, stdout, stderr = ssh_client.exec_command(sql_loader_command)
            #error=stderr.read()
            
            channel=ssh_client.invoke_shell()
            
            stdin = channel.makefile('wb')
            stdout = channel.makefile('rb') 
            stderr = channel.makefile_stderr('rb')
            stdin.write(sql_loader_command+'''
            exit
            ''')
            print (stdout.read())
            error=str(stderr.read())
            error=error.strip()
            print (error)
            
            if len(str(error))>3:
                #print (stderr.read())
                update_file_status_object_2=update_file_status()
                update_file_status_object_2.update_file_status('RE',identifier,'Caused by : sql loader did not work')
                raise ValueError(error)
            update_file_status_object_1=update_file_status()
            update_file_status_object_1.update_file_status('PR',identifier,'None',temp_table)
  
        except Exception as e:
            print (str(e))
        except ValueError as err:
            print(err.args)
        finally:
            stdout.close()
            stdin.close()