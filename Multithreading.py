import concurrent.futures as f
import asyncio
import time
from functionFile import postgresWriteTable
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
#conf = SparkConf().setAppName(value="datagenartion")
conf = SparkConf().setAppName(value="datagenartion").set('spark.scheduler.mode', 'FAIR').set("spark.scheduler.allocation.file", "C:/Users/mjoshi/PycharmProjects/pythonProject/fairscheduler.xml").set("spark.scheduler.pool", "production")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("WARN")




def readFiles1(path):
    #str(path).split("/")[-1:1:-1],
    df = spark.read.csv(path, inferSchema=True, header=True)
    time.sleep(1)
    print("---------------------", time.time())
    postgresWriteTable(spark, df, "jdbc:postgresql://localhost:5432/retail", "postgres", "postgres",
                       "org.postgresql.Driver", "append", "threadingData")

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
with f.ThreadPoolExecutor(max_workers=5) as executor:
    paths = ["C:/Users/mjoshi\mydata/tennis/tournaments_1877-2017_unindexed_csv.csv","C:/Users/mjoshi/mydata/tennis/match_scores_1877-1967_unindexed_csv.csv"
             ,"C:/Users/mjoshi/mydata/tennis/match_scores_1968-1990_unindexed_csv.csv","C:/Users/mjoshi/mydata/tennis/match_scores_1991-2016_unindexed_csv.csv"]
    executor.map(readFiles1,paths)
for path in paths:
    readFiles1(path)
time.sleep(999999)



#paths = ["C:/Users/mjoshi\mydata/tennis/tournaments_1877-2017_unindexed_csv.csv",
#          "C:/Users/mjoshi/mydata/tennis/match_scores_1877-1967_unindexed_csv.csv"
#     , "C:/Users/mjoshi/mydata/tennis/match_scores_1968-1990_unindexed_csv.csv",
#          "C:/Users/mjoshi/mydata/tennis/match_scores_1991-2016_unindexed_csv.csv"]
# for i in paths:
#     readFiles1(i)
# time.sleep(999999)