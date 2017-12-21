'''
Created on 2017Äê12ÔÂ18ÈÕ

@author: zhangwt
'''
import multiprocessing
import daemon
import time
import signal
import argparse
import os
import Queue
import sys
import datetime
import hashlib
import pandas as pd
from mutilscan import dyscan

PY3 = False
if sys.version_info < (2, 7) or sys.version_info > (4,):
    print("WARNING: You're running an untested version of python")
elif sys.version_info > (3,):
    PY3 = True
    
# Gets the directory that this file is in
MS_WD = os.path.dirname(os.path.abspath(__file__))
writepath = MS_WD+'/'

class scanProcess(multiprocessing.Process):
    def __init__(self, app, threadnums, whitepaths, blackpaths, md5):
        multiprocessing.Process.__init__(self)
        self.app = app
        self.threadnums = threadnums
        self.whitepaths = whitepaths
        self.blackpaths = blackpaths
        self.md5 = md5
    def outputpath(self):
        global writepath
        now = datetime.datetime.now().strftime('%Y%m%d%H')
        return writepath+self.app+'/'+now+'_'+self.md5+'/'+self.app+now+'.data'
 
    def run(self):
        dyscan.scan(self.app, self.threadnums, self.whitepaths, self.blackpaths, self.outputpath)

def getconf(file):
    tasks = []
    with open(file, 'r') as f:
        fileinfo = list(f)
    for i in range(len(fileinfo)):
        subtask = []
        if i%4 == 0:
            subtask = []
            subtask.append(fileinfo[i])
        elif i%4 == 1:
            if fileinfo[i] > 0:
                subtask.append(fileinfo[i])
            else:
                print('no valid threads number')
        elif i%4 == 2:
            q = Queue.Queue()
            for dir in fileinfo[i].split(';'):
                q.put(dir)
            subtask.append(q)   
        elif i%4 == 3:
            qb = Queue.Queue()
            for dir in fileinfo[i].split(';'):
                qb.put(dir)
            subtask.append(qb)
            tasks.append(subtask) 
        
    return tasks

def getfilemd5(filename):
    if not os.path.isfile(filename):
        return
    myhash = hashlib.md5()
    f = open(filename,'rb')
    while True:
        b = f.read(8096)
        if not b :
            break
        myhash.update(b)
    f.close()
    return myhash.hexdigest()

def timetoint(timestamp):
        time_local = time.localtime(timestamp)
        dt = time.strftime("%Y%m%d%H",time_local)
        return int(dt)


def _main():
    conf = ''
    while True:
        scantasks = getconf(conf)
        md5 = getfilemd5(conf)
        for task in scantasks:
            p = scanProcess(task[0], task[1], task[2], task[4], md5)
            p.start()
        time.sleep(3600)

if __name__ == '__main__':
    with daemon.DaemonContext():
        _main()