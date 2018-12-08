import paramiko
from classes.conn_details import conn_details

class create_qa_ctl_file(conn_details):
    def __init__(self):
       super(create_qa_ctl_file,self).__init__()
      
    def create_ctl_file(self,qa_file,qa_log,file,bad_file,discard_file,identifier,ssh_client):
        control_file = """OPTIONS (SKIP=1)
LOAD DATA
INFILE """  +file + """
BADFILE """ + bad_file + """
DISCARDFILE """ + discard_file + """
TRUNCATE INTO TABLE temp_"""+ identifier +"""
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' TRAILING NULLCOLS
(
id,
name,
salary
)"""    
 
        try:
             ftp = ssh_client.open_sftp()
             file=ftp.file(self.temp_dir+"/"+qa_file, "w", -1)
             file.write(control_file)
             
        except Exception as e:
            print("<ERROR> while ftp and writing file")
            print(str(e))
            
        finally:
            file.flush()
            ftp.close()
                