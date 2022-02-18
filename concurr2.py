from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from concurrent.futures import *
from functools import *
import pandas as pd
from pyspark.sql.types import *
import math
#conf = SparkConf().setAppName(value="datagenartion").set('spark.scheduler.mode', 'FAIR').set("spark.scheduler.allocation.file", "C:/Users/mjoshi/PycharmProjects/pythonProject/fairscheduler.xml").set("spark.scheduler.pool", "production")

sc = SparkContext()
spark = SparkSession(sc)
size = 4
ls = [i for i in range(1,61)]

cnt = math.ceil(len(ls)/size)
start=0
end=size
sub=[]

def transform(df,kls):
    print("--------->",kls)
    print(ls,df.show())

df = spark.read.csv("C:/data/emp.csv",header=True)
for i in range(0,cnt):
    sub.append(ls[start:end+1])
    start = end+1
    end = end+size
    print(end)
print(sub)
outdf = df.limit(0)
#spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
trnsfrm = partial(transform,df)
with ProcessPoolExecutor(max_workers=5) as executor:
    print("----------------executor started")
    executor.map(trnsfrm,ls)

pandasDF = outdf.toPandas()

# print(pandasDF)
# import time
import time
time.sleep(1000)
