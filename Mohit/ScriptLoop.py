import json
import subprocess
import psutil
from subprocess import CREATE_NEW_CONSOLE
import time
from datetime import datetime
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
import logging
logging.basicConfig(filename = 'autologCommands.log',level=logging.INFO,format='%(message)s')

with open("autologCommands.log", "r") as autolog:
    auto = autolog.read()
if("start_time,end_time,command" not in auto):
    logging.info("start_time,end_time,command,time_taken")

with open("conf2.json", "r") as config:
    conf = json.load(config)
command = conf["command"]
loop_num = len(command)
print(loop_num,type(loop_num))
concurrency = conf["concurrency"]
list_processes = {}
process = []
def concurrent_loop(command):
    print(" start at ",datetime.now()," of command ",command)
    start_time = datetime.now()
    print(command)
    child = subprocess.Popen(command, shell=True)
    print(child.pid)
    cmnicate = child.communicate()[0]
    print(" end at ",datetime.now()," of command ",command)
    end_time = datetime.now()
    timediff = end_time-start_time
    print(timediff.microseconds)
    logging.info("{},{},{},{}".format(str(start_time),str(end_time),command,str(timediff)))
    print("------------process {} completed------------------",child.pid)


def launch_concurrent_loop(command,loop_num):
    with ThreadPoolExecutor(max_workers=loop_num+1) as p:
        p.map(concurrent_loop,command)

def sequential_loop(command,loop_num):
    num=0
    print("------------------>",command,type(command))
    while(num<loop_num):
        print("command:--------",command)
        p = subprocess.Popen(command[num], shell=True, stdin=None, stdout=None, stderr=subprocess.PIPE, close_fds=True,universal_newlines=True)
        poller(p)
        num = num+1
        print("------------process {} completed------------------",num)

def poller(p):
    pl = p.poll()
    if pl is None:
        print("running, ",p.pid)
        time.sleep(1)
        poller(p)

if(str(concurrency).lower()=="yes"):
    launch_concurrent_loop(command,loop_num)
elif(str(concurrency).lower()=="no"):
    sequential_loop(command,loop_num)
    #tracer(list_processes)