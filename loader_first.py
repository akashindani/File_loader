import threading

from classes.conn_details import conn_details
from classes.monitor_csv_thread import monitor_csv_thread
from classes.monitor_xml_thread import monitor_xml_thread
from classes.load_csv_thread import load_csv_thread
from classes.load_xml_thread import load_xml_thread



if __name__=='__main__':
    print("Main started")   
    c1=conn_details()
    print (c1.get_host_name())   
    
    monitor_csv_thread_object=monitor_csv_thread()
    thread1=threading.Thread(target=monitor_csv_thread_object.trigger_thread,name='thread1')
    thread1.start()    
    
    monitor_xml_thread_object=monitor_xml_thread()
    thread2=threading.Thread(target=monitor_xml_thread_object.trigger_thread,name='thread2')
    thread2.start()

    load_csv_thread_object=load_csv_thread()
    thread3=threading.Thread(target=load_csv_thread_object.trigger_thread,name='thread3')
    thread3.start()

    load_xml_thread_object=load_xml_thread()
    thread4=threading.Thread(target=load_xml_thread_object.trigger_thread,name='thread4')
    thread4.start()
   
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()    
    #m1.monitor_for_new_csv_file()
    #m1.monitor_for_new_xml_file()
    #l1=load_csv()
    #l1.csv_loader()    
    print("Main completed")
    
