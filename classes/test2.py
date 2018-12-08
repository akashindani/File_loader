import paramiko

client = paramiko.SSHClient()
client.load_system_host_keys()
client.set_missing_host_key_policy(paramiko.WarningPolicy)
client.connect(hostname='illnqw1404', username='abpwrk1', password='Unix11!')

channel=client.invoke_shell()

stdin = channel.makefile('wb')
stdout = channel.makefile('rb')
stderr = channel.makefile_stderr('rb')

stdin.write('''
cd akash
ls
exit
''')
print (stdout.read())
if len(stderr.read())>0:
    print (stderr.read())

stdout.close()
stdin.close()
client.close()