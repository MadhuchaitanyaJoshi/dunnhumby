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
with f.ThreadPoolExecutor(max_workers=1) as executor:
    paths = ["C:/Users/mjoshi\mydata/tennis/tournaments_1877-2017_unindexed_csv.csv","C:/Users/mjoshi/mydata/tennis/match_scores_1877-1967_unindexed_csv.csv"
             ,"C:/Users/mjoshi/mydata/tennis/match_scores_1968-1990_unindexed_csv.csv","C:/Users/mjoshi/mydata/tennis/match_scores_1991-2016_unindexed_csv.csv"]
    executor.map(readFiles1,paths)
# paths = ["C:/Users/mjoshi\mydata/tennis/tournaments_1877-2017_unindexed_csv.csv","C:/Users/mjoshi/mydata/tennis/match_scores_1877-1967_unindexed_csv.csv"
#              ,"C:/Users/mjoshi/mydata/tennis/match_scores_1968-1990_unindexed_csv.csv","C:/Users/mjoshi/mydata/tennis/match_scores_1991-2016_unindexed_csv.csv"]
# for path in paths:
#     readFiles1(path)
# time.sleep(999999)



# paths = ["C:/Users/mjoshi\mydata/tennis/tournaments_1877-2017_unindexed_csv.csv",
#          "C:/Users/mjoshi/mydata/tennis/match_scores_1877-1967_unindexed_csv.csv"
#     , "C:/Users/mjoshi/mydata/tennis/match_scores_1968-1990_unindexed_csv.csv",
#          "C:/Users/mjoshi/mydata/tennis/match_scores_1991-2016_unindexed_csv.csv"]
# for i in paths:
#     readFiles1(i)
# time.sleep(999999)

#     sampledf = sampledf.withColumn(conf[db][table]["maskcolumns"][colm[0]],
#                                    when(sampledf[colm[0]].isNotNull, abs(hash(sampledf[colm[0]]))).otherwise(
#                                        hash(sampledf[colm[0]])))
# if (colm[0] in mstcols and colm[1] == "string"):
#     print("27------------", basecol[colm[0]])
#     sampledf = sampledf.withColumn(basecol[colm[0]], sha2((sampledf[colm[0]]), 256))
