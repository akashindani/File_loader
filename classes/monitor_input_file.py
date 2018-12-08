import paramiko
import cx_Oracle

from classes.conn_details import conn_details

class monitor_input_file(conn_details):
    def __init__(self):
        super(monitor_input_file,self).__init__()


    def monitor_for_new_csv_file(self):
        
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy)
        try:
            client.connect(hostname='illnqw1404', username='abpwrk1', password='Unix11!')
            
            input_dir_path=self.get_input_dir_path()
            temp_dir=self.get_temp_dir_path()
            input_files='ls '+input_dir_path+'/*.csv | cut -d \'/\' -f9 | awk \'{$1=$1;print}\''
            #print(input_files)
            #os.system(input_files)  
            try:      
                file_names=""
                stdin, stdout, stderr = client.exec_command(input_files)
                file_names=stdout.read()
                file_names_1=file_names.decode('utf-8')
                #print (type(file_names_1))
                file_names_1=file_names_1.strip()
                print("Files : " +file_names_1 )
                if len(file_names_1)>0 :
                    for f,file in enumerate(file_names_1.split('\n')):
                        #print (type(file))
                        print ("file_name = "+file )
                        
                        seq_nextval_sql='select file_control_seq.NEXTVAL from dual'  
                        
                        try:
                            con = cx_Oracle.connect('ABPAPP1/ABPAPP1@illnqw1404/CMDDB1404')
                            cur = con.cursor()
                            cur.prepare(seq_nextval_sql)
                            cur.execute(None)
                            resultset = cur.fetchall()
                            t1=resultset[0]
                            sequence_next_val=str(t1[0])
                            
                        except Exception as e:
                               print ("<ERROR> SQL of : "+ str(e))
                                
                        finally:
                            cur.close()
                            con.close()
                        
                        copy_to_temp_command='cp '+input_dir_path+'/'+file+' '+temp_dir+"/"+file.replace('.csv','')+"_"+sequence_next_val+".csv"
                        stdin, stdout, stderr = client.exec_command(copy_to_temp_command)
                        error = stderr.read()
                        if len(error)  > 0:
                            print("<ERROR> " + error.decode('utf-8'))
                        
                        
                        else:
                            
                            mark_comp_command='mv '+input_dir_path+'/'+file+' '+input_dir_path+'/'+file+'.comp'
                            stdin, stdout, stderr = client.exec_command(mark_comp_command)
                            error = stderr.read()
                            if len(error)  > 0:
                                print("<ERROR> " + error.decode('utf-8'))
                            
                            file=file.replace('.csv','')+"_"+sequence_next_val+".csv"
                            
                            insert_sql = "insert into file_control (identifier,sys_creation_date,sys_update_date,file_name,file_path,file_status,file_type) values ( "+sequence_next_val+",sysdate,sysdate,:1,'/users/gen/abpwrk1/akash/python/loader/temp','RD','csv')"
                            try:
                                con = cx_Oracle.connect('ABPAPP1/ABPAPP1@illnqw1404/CMDDB1404')
                                cur = con.cursor()
                                cur.prepare(insert_sql)
                                cur.execute(None,{'1':file})
                                con.commit()
                                print (con.version)
                            
                            except Exception as e:
                                print ("<ERROR> SQL : "+ str(e))
                                
                            finally:
                                cur.close()
                                con.close()
                else:
                    print ("<Py INFO> There were no new files to insert")
                         
                
                error = stderr.read()
                if len(error)  > 0:
                    print("<Unix ERROR> " + error.decode('utf-8'))
                #file_to_insert='insert_new_file.sql'
                
                #if os.path.isfile(input_dir_path+'/'+file_to_insert):
                           # os.remove(input_dir_path+'/'+file_to_insert)
            except Exception as e:
                print ("<ERROR> "+ str(e))
                
        except paramiko.SSHException:
            print ("<ERROR> Unable to do the ssh to machine, check if machine details are correct" )
        except Exception as e:
            print ("<ERROR> "+ str(e))
        
        finally:
            client.close()
    
    def monitor_for_new_xml_file(self):
        
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy)
        try:
            client.connect(hostname='illnqw1404', username='abpwrk1', password='Unix11!')
            
            input_dir_path=self.get_input_dir_path()
            temp_dir=self.get_temp_dir_path()
            input_files='ls '+input_dir_path+'/*.xml | cut -d \'/\' -f9 | awk \'{$1=$1;print}\''
            #print(input_files)
            #os.system(input_files)  
            try:      
                file_names=""
                stdin, stdout, stderr = client.exec_command(input_files)
                file_names=stdout.read()
                file_names_1=file_names.decode('utf-8')
                #print (type(file_names_1))
                file_names_1=file_names_1.strip()
                print("Files : " +file_names_1 +" len : ")
                if len(file_names_1)>0 :
                    for f,file in enumerate(file_names_1.split('\n')):
                        #print (type(file))
                        print ("file_name = "+file )
                        
                        seq_nextval_sql='select file_control_seq.NEXTVAL from dual'  
                        
                        try:
                            con = cx_Oracle.connect('ABPAPP1/ABPAPP1@illnqw1404/CMDDB1404')
                            cur = con.cursor()
                            cur.prepare(seq_nextval_sql)
                            cur.execute(None)
                            resultset = cur.fetchall()
                            t1=resultset[0]
                            sequence_next_val=str(t1[0])
                            
                        except Exception as e:
                                print ("<ERROR> SQL of : "+ str(e))
                                
                        finally:
                            cur.close()
                            con.close()
                        print (file)
                        copy_to_temp_command='cp '+input_dir_path+'/'+file+' '+temp_dir+"/"+file.replace('.xml','')+"_"+sequence_next_val+".xml"
                        print (copy_to_temp_command)
                        stdin, stdout, stderr = client.exec_command(copy_to_temp_command)
                        error = stderr.read()
                        if len(error)  > 0:
                            print("<ERROR> " + error.decode('utf-8'))
                        
                        else:
                            mark_comp_command='mv '+input_dir_path+'/'+file+' '+input_dir_path+'/'+file+'.comp'
                            stdin, stdout, stderr = client.exec_command(mark_comp_command)
                            error = stderr.read()
                            if len(error)  > 0:
                                print("<ERROR> " + error.decode('utf-8'))
                            
                            file=file.replace('.xml','')+"_"+sequence_next_val+".xml"
                            
                            insert_sql = "insert into file_control (identifier,sys_creation_date,sys_update_date,file_name,file_path,file_status,file_type) values ( "+sequence_next_val+" ,sysdate,sysdate,:1,'/users/gen/abpwrk1/akash/python/loader/temp','RD','xml')"
                            try:
                                con = cx_Oracle.connect('ABPAPP1/ABPAPP1@illnqw1404/CMDDB1404')
                                cur = con.cursor()
                                cur.prepare(insert_sql)
                                cur.execute(None,{'1':file})
                                con.commit()
                                print (con.version)
                            
                            except Exception as e:
                                print ("<ERROR> SQL: "+ str(e))
                                
                            finally:
                                cur.close()
                                con.close()
                else:
                    print ("<Py INFO> There were no new files to insert")
                         
                
                error = stderr.read()
                if len(error)  > 0:
                    print("<Unix ERROR> " + error.decode('utf-8'))
                #file_to_insert='insert_new_file.sql'
                
                #if os.path.isfile(input_dir_path+'/'+file_to_insert):
                           # os.remove(input_dir_path+'/'+file_to_insert)
            except Exception as e:
                print ("<ERROR> "+ str(e))
                
        except paramiko.SSHException:
            print ("<ERROR> Unable to do the ssh to machine, check if machine details are correct" )
        except Exception as e:
            print ("<ERROR> "+ str(e))
        
        finally:
            client.close()
    
    def insert_new_file(self):
        pass
    
    def update_file_status(self):
        pass
    
    def mark_file_comp(self):
        pass
    
    
        
     
            
    
    