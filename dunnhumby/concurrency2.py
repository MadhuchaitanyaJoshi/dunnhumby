from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from concurrent.futures import *
import pandas as pd
from pyspark.sql.types import *
import math
conf = SparkConf().setAppName(value="datagenartion").set('spark.scheduler.mode', 'FAIR').set("spark.scheduler.allocation.file", "C:/Users/mjoshi/PycharmProjects/pythonProject/fairscheduler.xml").set("spark.scheduler.pool", "production")

sc = SparkContext()
spark = SparkSession(sc)
size = 4
ls = [i for i in range(1,61)]

cnt = math.ceil(len(ls)/size)
#print(ls)
#print(cnt)
start=0
end=size
sub=[]
def transform(df):
 #   print("--------->",k)
#    print(ls,df.show())
#    print("inside transoform")
    df= df.filter(df["eid"].isin(ls))
#    print(df.show())
    outdf = df.unionAll(df)
    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$",outdf.show())

df = spark.read.csv("C:/data/emp.csv",header=True)
for i in range(0,cnt):
    sub.append(ls[start:end+1])
    start = end+1
    end = end+size
    print(end)
outdf = df.limit(0)
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
with ProcessPoolExecutor(max_workers=20) as executor:
    result = [executor.submit(transform, df,i) for i in sub]

# for i in sub:
#     transform(df,i)

#print("**************************************",outdf,outdf.count())
pandasDF = outdf.toPandas()

print(pandasDF)
import time
time.sleep(1000)
