import concurrent.futures as f
import asyncio
import os
import time
from functools import *
from functionFile import postgresWriteTable
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import math
conf = SparkConf().setAppName(value="datagenartion")
#conf = SparkConf().setAppName(value="datagenartion").set('spark.scheduler.mode', 'FAIR').set("spark.scheduler.allocation.file", "C:/Users/mjoshi/PycharmProjects/pythonProject/fairscheduler.xml").set("spark.scheduler.pool", "production")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("WARN")


def trans(df,task):
    print("started---------->",os.getpid())
    print("task is:---------->",task)
    df2 = df.filter(df["eid"].isin(task))
    print(df2.show())
    print("ended---------->", os.getpid())


# ls=[[1,2,3,4,5],[6,7,8,9,10]]

ls = [i for i in range(1,61)]

size = 4
cnt = math.ceil(len(ls)/size)
print(ls)
print(cnt)
start=0
end=size
sub=[]
for i in range(0,cnt):
    sub.append(ls[start:end+1])
    start = end+1
    end = end+size
    print(end)
print(sub)


emp = spark.read.csv("C:/data/emp.csv",header=True,inferSchema=True)
with f.ThreadPoolExecutor() as executor:
    part = partial(trans,emp)
    executor.map(part,sub)


time.sleep(99999)