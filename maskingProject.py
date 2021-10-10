import json
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkContext()
spark = SparkSession(sc)


def hiveReadTable():
    pass


def postgresReadTable(in_url_connect, in_user, in_password, in_driver, in_table):
    properties = {"user": in_user, "password": in_password, "driver": in_driver}
    print("----------->",in_table,table)
    df = spark.read.jdbc(url=in_url_connect, table=in_table, properties=properties)
    return df


def bigqueryReadTable():
    pass


def hiveWriteTable():
    pass


def postgresWriteTable():
    pass


def bigqueryWriteTable():
    pass


with open("C:/Users/mjoshi/PycharmProjects/pythonProject/masterColumns.txt", "r") as mstcolumns:
    mstcols = mstcolumns.read().split(",")


class dbProperties:
    def __init__(self, db, table, maskedcolumns, fraction, inputInfo, outInfo, error_log):
        self.db = db
        self.table = table
        self.maskedcolumns = maskedcolumns
        self.fraction = fraction
        self.inputInfo = inputInfo
        self.outInfo = outInfo
        self.error_log = error_log

    def printer(self):
        print(self.db)
        print(self.table)
        print(self.maskedcolumns)
        print(self.fraction)
        print(self.inputInfo)
        print(self.outInfo)


with open("C:/Users/mjoshi/PycharmProjects/pythonProject/config.json", "r") as config:
    conf = json.load(config)
ls = []
for db in conf:
    for table in conf[db]:
        ls.append(dbProperties(db, table, conf[db][table]["maskcolumns"], conf[db][table]["fraction"],
                               conf[db][table]["inputInfo"], conf[db][table]["outInfo"], conf[db][table]["error_log"]))

for obj in ls:
    inengine = obj.inputInfo["inengine"]
    in_url_connect = obj.inputInfo["url_connect"]
    in_user = obj.inputInfo["user"]
    in_password = obj.inputInfo["password"]
    in_driver = obj.inputInfo["driver"]
    in_db = obj.db
    in_table = obj.table
    fraction = obj.fraction
    print("table : ------------>",obj.table)
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(),StructType([]))
    if inengine == "postgres":
        df = postgresReadTable(in_url_connect, in_user, in_password, in_driver, in_table)
    sampledf = df.sample(fraction)
    while sampledf.count() == 0 or sampledf.count() > (df.count() * fraction):
        sampledf = df.sample(fraction)
    cols = df.dtypes
    for colm in cols:
        basecol = obj.maskedcolumns
        if colm[0] in mstcols and colm[1] == "int":
            sampledf = sampledf.withColumn(basecol[colm[0]],
                                            when(hash(sampledf[colm[0]]) < 0,
                                                hash(sampledf[colm[0]]) * -1).otherwise(hash(sampledf[colm[0]])))
        if colm[0] in mstcols and colm[1] == "string":
            sampledf = sampledf.withColumn(basecol[colm[0]], sha2((sampledf[colm[0]]), 256))
