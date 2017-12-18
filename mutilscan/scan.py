# -*- coding:utf-8 -*-
import sys
import unidecode
import time
import signal
import argparse
import os
import Queue
import threading
import datetime
import logging
import pandas as pd
from influxdb import DataFrameClient

start = ''
thread_count = 0
pathq = Queue.Queue()
allfiles = 0
logging.basicConfig(filename='queue.log', filemode="w", level=logging.INFO)
lock = threading.Lock()
class influxThread(threading.Thread):
    def __init__(self, number):
        threading.Thread.__init__(self)
        self.tnumber = number
        self.arrange = number
        
    def next(self):
        global thread_count
        self.arrange = self.arrange + 1
        if(self.arrange == thread_count):
            self.arrange = 0
        return self.arrange
    
    def timetoint(self, timestamp):
        time_local = time.localtime(timestamp)
        dt = time.strftime("%Y%m%d",time_local)
        return int(dt)
    
    def timetos(self, timestamp):
        time_local = time.localtime(timestamp)
        dt = time.strftime("%Y%m%d%H%M%S",time_local)
        return int(dt)
    
    def run(self):
        global allfiles
        global thread_count
        global pathq
        empty = False
        filesize = 0
        path, size, filetype, modify_time, create_time, access_time, scan_time, uid, gid = [], [], [], [], [], [], [], [], []
        while True:  
            filelist = []
            try:
                scanpath = pathq.get()
                logging.info('start at: '+start+ "\n")
                logging.info(scanpath+ "\n")
                if os.path.isdir(scanpath):
                    filelist = os.listdir(scanpath)
                else:
                    print(scanpath+'is not '+'valid path')
                    continue
                if len(filelist) > 100000:
                    print(scanpath+' has: '+str(len(filelist))+' files'+', do not scan now')
                    continue
                for file in filelist:
                    filename = os.path.join(scanpath,file)
                    if os.path.exists(filename):
                        if os.path.isdir(filename):
                            pathq.put(filename)
                        elif os.path.isfile(filename):
                            lock.acquire()
                            allfiles += 1
                            print(allfiles)
                            lock.release()
                            fileinfo = os.stat(filename)
                            size.append(fileinfo.st_size)
                            filetype.append(fileinfo.st_type)
                            modify_time.append(self.timetos(fileinfo.st_mtime))
                            create_time.append(self.timetos(fileinfo.st_ctime))
                            access_time.append(self.timetoint(fileinfo.st_atime))
                            scan_time.append(datetime.datetime.now())
                            uid.append(fileinfo.st_uid)
                            gid.append(fileinfo.st_gid)
                            filename = unidecode.unidecode_expect_nonascii(filename)
                            path.append(filename)
                            filesize += 1
                            if filesize == 50000:
                                print("Write InfluxDB...")
                                df = pd.DataFrame({"path" : path,"size": size,"filetype": filetype,"modify_time": modify_time,"create_time": create_time,"access_time" : access_time,
                                                   "scan_time": scan_time,"uid": uid,"gid": gid}, index=pd.date_range(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), freq='U', periods=len(path)))
                                client.write_points(df, 'infos', protocol='json')
                                print("Write Done")
                                logging.info('start at: '+start)
                                logging.info('end at: '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                                logging.info("filesize: %s" % allfiles)
                                path, size, filetype, modify_time, create_time, access_time, scan_time, uid, gid = [], [], [], [], [], [], [], [], []
                                filesize = 0                
            except Queue.Empty as e:
                empty = True
                print("Write InfluxDB...")
                df = pd.DataFrame({"path" : path,"size": size,"filetype": filetype,"modify_time": modify_time,"create_time": create_time,"access_time" : access_time,
                                                   "scan_time": scan_time,"uid": uid,"gid": gid}, index=pd.date_range(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), freq='U', periods=len(path)))
                client.write_points(df, 'infos', protocol='json')
                print("Write Done")
                logging.info('start at: '+start)
                logging.info('end at: '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                logging.info("filesize: %s" % allfiles)
                path, size, filetype, modify_time, create_time, access_time, scan_time, uid, gid = [], [], [], [], [], [], [], [], []                 
                print 'scanning end, now you can enter ctrl+c to stop this program'
                break   
            except OSError as e:
                print 'OSError:', e
                continue
            except Exception as e:
                print 'Exception:', e
                continue
            finally:
                if not empty:
                    pathq.task_done()
            
                             

                     
        
def quit(signum, frame):
    print 'You choose to stop me.'
    print('start at: '+start)
    print('end at: '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print "allfiles: %s" % allfiles
    sys.exit()

def mutilscan(host, port, path):
    """Instantiate a connection to the InfluxDB."""
    user = 'root'
    password = 'root'
    dbname = 'test'
    thread_count = 10
    global start
    start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    global client
    client = DataFrameClient(host, port, user, password, dbname)
    client.create_database(dbname) 
    print("scanning. please wait...")
    
    for i in range(thread_count):
        t = influxThread(i)
        t.setDaemon(True)
        t.start()
    pathq.put(path)

    
    