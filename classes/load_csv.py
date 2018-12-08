import paramiko
import cx_Oracle


from classes.conn_details import conn_details
from classes.update_file_status import update_file_status
from classes.create_qa_ctl_file import create_qa_ctl_file
from classes.run_csv_sqlldr import run_csv_sqlldr

class load_csv(conn_details):
    def __init__(self):
        super(load_csv,self).__init__()
    
    def csv_loader(self):

        select_xml_rd_files='select identifier,file_name from file_control where file_status=\'RD\' and file_type=\'csv\' '
        
        try:
            ssh_client =paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname='illnqw1404', username='abpwrk1', password='Unix11!')                          
            con = cx_Oracle.connect('ABPAPP1/ABPAPP1@illnqw1404/CMDDB1404')
            cur = con.cursor()
            cur.prepare(select_xml_rd_files)
            cur.execute(None)
            #resultset = cur.fetchall()
            #print(resultset)   
            
            for row in cur.fetchall():
                
                identifier=str(row[0]).strip()
                print (identifier)
                file=row[1].strip()
                file=str(file)
                #file=file.replace('.csv','_'+identifier+'.csv')
                print (file)
                bad_file=file.replace('.csv','.bad')
                discard_file=file.replace('.csv','.dsc')
                qa_file="qa_"+identifier+".ctl"
                qa_log="qa_"+identifier+".log"
                temp_table="temp_"+identifier
                    
                try:
                    create_qa_ctl_file_object=create_qa_ctl_file()
                    create_qa_ctl_file_object.create_ctl_file(qa_file,qa_log,file,bad_file,discard_file,identifier,ssh_client) 
                except Exception as e :
                    print ("caused by : failed while creating object or calling ctl function")
                    print (str(e))
                    update_file_status_object_1=update_file_status()
                    update_file_status_object_1.update_file_status('RE',identifier,'caused by : failed while creating object or calling ctl function')
                    continue

                try:
                    drop_temp_table="drop table "+temp_table
                
                    cur.prepare(drop_temp_table)
                    cur.execute(None)
                except Exception as e:
                    print ("<ERROR> SQL of : "+ str(e))
                
                try:
                    temp_table_sql="create table "+temp_table+" ( id varchar2(100), name varchar2(100), salary varchar2(100))"
                    cur.prepare(temp_table_sql)
                    cur.execute(None)
                except Exception as e :
                    print ("caused by : Not able to create the temp table")
                    raise ValueError(str(e))
                    
                try:
                    run_csv_sqlldr_object=run_csv_sqlldr()
                    run_csv_sqlldr_object.run_sqlldr(qa_file,qa_log,identifier,temp_table,ssh_client)
                except Exception as e:
                    print("<ERROR> while calling sqlloader function in python")
                    print(str(e))

        except Exception as e:
            print ("<ERROR> SQL of : "+ str(e))
        except ValueError as error:
            print(error.args)
            
        finally:
            cur.close()
            con.close()
            ssh_client.close()
        
        