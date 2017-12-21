# -*- coding:utf-8 -*-
import sys
import unidecode
import time
import os
import Queue
import threading
import datetime
import logging
import pandas as pd
import json

pathq = Queue.Queue()
allfiles = 0
logging.basicConfig(filename='scanerror.log', filemode="w", level=logging.INFO)
lock = threading.Lock()
thispath = os.path.abspath('.')

class scandir(object):
    def __init__(self, name, filelist):
        self.name = name
        self.filelist = filelist

class scanfile(object):
    def __init__(self, tp, sz, mt, at, ct, st, uid, gid, fn):
        self.tp = tp
        self.sz = sz
        self.mt = mt
        self.at = at
        self.ct = ct
        self.st = st
        self.uid = uid
        self.gid = gid
        self.fn = fn

def scan(app, threadnums, whitepaths, blackpaths, outputpath):
    global allfiles
    global pathq
    pathq = whitepaths
    filelist = []
    try:
        scanpath = pathq.get()
        if scanpath not in blackpaths and os.path.isdir(scanpath):
            filelist = os.listdir(scanpath)
            if len(filelist) < 100000:
                flist= []
                for file in filelist:
                    filename = os.path.join(scanpath,file)
                    if os.path.exists(filename):
                        if os.path.isdir(filename):
                            pathq.put(filename)
                            if threading.activeCount() < threadnums:
                                t = threading.Thread(target=scan)
                                t.start()
                        elif os.path.isfile(filename):
                            lock.acquire()
                            allfiles += 1
                            print(app+':%s' %allfiles)
                            lock.release()
                            fileinfo = os.stat(filename)
                            f = scanfile(fileinfo.st_type, fileinfo.st_size, fileinfo.st_mtime, fileinfo.st_atime,
                                      fileinfo.st_ctime, datetime.datetime.now(), fileinfo.st_uid, fileinfo.st_gid, unidecode.unidecode_expect_nonascii(filename))
                            flist.append(f)
                dir = scandir(scanpath, flist)
                with open(outputpath, 'w') as fw:
                    fw.write(json.dumps(dir, default=lambda obj: obj.__dict__))
                flist= []                
            else:
                print(scanpath+' has: '+str(len(filelist))+' files'+', do not scan now')
        else:
            print(scanpath+'is not '+'valid path')
                    
    except OSError as e:
        print 'OSError:', e
        logging.info('error at: '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'in'+scanpath+'OSError:'+e)
    except Exception as e:
        print 'Exception:', e
        logging.info('error at: '+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'in'+scanpath+'Exception:'+e)