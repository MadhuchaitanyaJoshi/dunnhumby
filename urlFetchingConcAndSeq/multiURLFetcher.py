import json
import psutil
import time
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen

with open("/home/mmishra/Desktop/Url_fetch/venv/conf.json", "r") as config:
    conf = json.load(config)
command = conf["command"]
loop_num = conf["loop_num"]
print(loop_num,type(loop_num))
concurrency = conf["concurrency"]
list_processes = {}
process = []
filename = "Multiurl.csv"


def urlFetch(url):
    fields = ['Start_time', 'URL', 'End_Time']
    #print(" start at ",datetime.now()," of url ",url)
    s_time=datetime.now()
    response = urlopen(url)
    data_json = json.loads(response.read())
    e_time=datetime.now()
    #print(data_json)
    # name of csv file


    # writing to csv file

        # creating a csv writer object
    print("Start_time",start_time,"End_time",end_time)
    csvwriter = csv.writer(csvfile)

        # writing the fields
    csvwriter.writerow(fields)

        # writing the data rows

    csvwriter.writerows(s_time)
    csvwriter.writerows(url)
    csvwriter.writerows(e_time)
    print(" end at ",datetime.now()," of url ",url)

def concurrent_loop(command):
    urlFetch(command)

def launch_concurrent_loop(command):
    #lst_commands= [command for i in range(0,loop_num)]
    #print(lst_commands)
    with ThreadPoolExecutor(max_workers=len(command)) as executor:
        executor.map(concurrent_loop,command)

def sequential_loop(command):
    num=0
    ln_cmd = len(command)
    while(num<ln_cmd):
        #p = subprocess.Popen(command, shell=True, stdin=None, stdout=None, stderr=subprocess.PIPE, close_fds=True,universal_newlines=True)
        #poller(p)
        urlFetch(command[num])
        num = num+1
        print("------------process {} completed------------------",num)

def poller(p):
    pl = p.poll()
    if pl is None:
        print("running, ",p.pid)
        time.sleep(1)
        poller(p)

with open(filename, 'a+') as csvfile:

    if(str(concurrency).lower()=="yes"):
        launch_concurrent_loop(command)
        #tracer(list_processes)
    elif(str(concurrency).lower()=="no"):
        sequential_loop(command)
        #tracer(list_processes)
