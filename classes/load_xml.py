import paramiko
import cx_Oracle

from classes.conn_details import conn_details
from classes.update_file_status import update_file_status

class load_xml(conn_details):
    def __init__(self):
        super(load_xml,self).__init__()
    
    def xml_loader(self):
        select_xml_rd_files='select identifier,file_path,file_name from file_control where file_status=\'RD\' and file_type=\'xml\' '
        
        try:
            ssh_client =paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname='illnqw1404', username='abpwrk1', password='Unix11!')                          
            con = cx_Oracle.connect(self.app_db_conn_str)
            cur = con.cursor()
            cur.prepare(select_xml_rd_files)
            cur.execute(None)
            #resultset = cur.fetchall()
            #print(resultset)   
            
            for row in cur.fetchall():
                identifier= str(row[0]).strip()
                file_path=str(row[1]).strip()
                file_name=str(row[2]).strip()
                
                print (identifier)
                print (file_path)
                print (file_name)
                
                sftp_client = ssh_client.open_sftp()
                remote_file = sftp_client.open(file_path+"/"+file_name)
                input_xml_content=''
                try:
                    for line in remote_file:
                        print (line)
                        input_xml_content=input_xml_content+str(line)
                
                except Exception as e:
                    print(str(e))
                    update_file_status_object_1=update_file_status()
                    update_file_status_object_1.update_file_status('RE',identifier,'caused by : failed accesing the file')
                
                finally:
                    print (input_xml_content)
                    remote_file.close()
                
                try:
                    
                    try:
                        drop_full_xml='drop table temp_full_xml'
                        cur.prepare(drop_full_xml)
                        cur.execute(None)
                    except Exception as e:
                        print(str(e))
                        
                    create_full_xml='CREATE TABLE temp_full_xml (identifier varchar2(500),xml_data  XMLTYPE)'
                    cur.prepare(create_full_xml)
                    cur.execute(None)
                    insert_into_temp_full_xml='insert into temp_full_xml (xml_data) values (xmltype(\''+input_xml_content+'\'))'
                    cur.prepare(insert_into_temp_full_xml)
                    cur.execute(None)
                    con.commit()
                
                except Exception as e:
                    print(str(e))
                    update_file_status_object_1=update_file_status()
                    update_file_status_object_1.update_file_status('RE',identifier,'caused by : failed at inserting to temp_full_xml')
                    
                try:                   
                    try:
                        
                        temp_table = 'temp_'+identifier
                        drop_temp_table='drop table '+temp_table
                        cur.prepare(drop_temp_table)
                        cur.execute(None)
                    except Exception as e:
                        print(str(e))
                    
                    create_temp_table='create table '+temp_table+' as select trim(extract(xml_data,\'//Record/ID/text()\')) as id,trim(extract(xml_data,\'//Record/Name/text()\')) as Name,trim(extract(xml_data,\'//Record/Salary/text()\')) as salary from temp_full_xml'
                    cur.prepare(create_temp_table)
                    cur.execute(None)
                    update_file_status_object_1=update_file_status()
                    update_file_status_object_1.update_file_status('PR',identifier,'None',temp_table)
                                    
                except Exception as e:
                    print(str(e))
                    update_file_status_object_1=update_file_status()
                    update_file_status_object_1.update_file_status('RE',identifier,'caused by : failed wile creating temp table temp_<identifier>')
           
                
        except Exception as e:
            print(str(e))
        finally:
            cur.close()
            con.close()
            ssh_client.close()