import os
import signal
import subprocess
import redis
import sys
import threading
import json
import struct
import time
import numpy
import glob
import signal
import datetime
import pickle
import os.path



def start_process(testfile):

    detect_proc = subprocess.Popen(['python3 NN.py ' + testfile], shell=True, preexec_fn=os.setsid, close_fds=True)
    print(detect_proc.pid)
    return detect_proc.pid

# starts python script that collects memory and cpu utilization
# receives the pid to be monitored, the monitoring period (in secs), and a filename in which collected data is written as parameters
def start_resource_monitor(filename, testfile, period):
    pid = start_process(testfile)
    global resource_proc
    param = "-f " + filename + " -mp " + str(period)
    resource_proc = subprocess.Popen(['python3 resource-monitor.py ' + str(pid) + ' ' + param], shell=True, preexec_fn=os.setsid, close_fds=True)
    return resource_proc


if __name__ == '__main__':
    print("Starting at", datetime.datetime.now())

    filename=sys.argv[1]
    testfile = sys.argv[2]
    period = 0.5
    #if os.path.isfile('memotable.pickle'):
    #    with open('memotable.pickle', 'rb') as m:
    #        memoization_table = pickle.load(m)

    #binary_search(sys.argv[1], sys.argv[2])
    start_resource_monitor(filename, testfile, 0.5)
    print("Finishing at", datetime.datetime.now())
                                                        
