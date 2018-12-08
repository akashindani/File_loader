import paramiko
import cx_Oracle
import conn_details

con = cx_Oracle.connect('ABPAPP1/ABPAPP1@illnqw1404/CMDDB1404')
cur = con.cursor()
print (con.version)
list =[]

cur.execute('select * from customer where rownum <2')

for result1 in cur.fetchall():
    list.append(result1)
    print ("finally, it works!!!")
    print (list)

engine = create_engine('oracle://[ABPAPP1]:[ABPAPP1]@[illnqw1404]:[CMDDB1404]', echo=False)


try:
    command='asdasd'
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.WarningPolicy)
    
    client.connect(hostname='illnqw1404', username='abpwrk1', password='Unix11!')
    
    stdin, stdout, stderr = client.exec_command(command)
    print (stdout.read())
    error = stderr.read()
    if len(error)  > 0:
        print("<ERROR> " + error.decode('utf-8'))


finally:
    client.close()