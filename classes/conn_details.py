class conn_details(object):
    def __init__(self):
        
        self.file_system='/users/gen/'
        self.hostname='illnqw1404'
        self.username='abpwrk1'
        self.password='Unix11!'
        self.log_path=self.file_system+self.username+'/akash/python/loader/logs'
        self.input_dir=self.file_system+self.username+'/akash/python/loader/input'
        self.output_dir=self.file_system+self.username+'/akash/python/loader/output'
        self.temp_dir=self.file_system+self.username+'/akash/python/loader/temp'
        self.app_db_conn_str='ABPAPP1/ABPAPP1@illnqw1404/CMDDB1404'
        self.sqlloader_db_con_str='ABPAPP1/ABPAPP1@CMDDB1404'
        
    def get_host_name(self):
        return self.hostname
        
    def get_username(self):
        return self.username
    
    def get_log_path(self):
        return self.log_path
        
    def get_input_dir_path(self):
        return self.input_dir
        
    def get_output_dir_path(self): 
        return self.output_dir
        
    def get_temp_dir_path(self):
        return self.temp_dir
    
    
    