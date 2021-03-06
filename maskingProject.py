import json
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession,HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
conf = SparkConf().setAppName(value="maskingDataGeneration")
sc = SparkContext(conf = conf)
spark = SparkSession(sc).builder.enableHiveSupport().getOrCreate()

hiveContext = HiveContext(sc)

def hiveReadTable(in_db,in_table):
    df = spark.sql("select * from {}.{}".format(in_db,in_table))
    return df

def postgresReadTable(in_url_connect, in_user, in_password, in_driver, in_table):
    properties = {"user": in_user, "password": in_password, "driver": in_driver}
    print("----------->",in_table,table)
    df = spark.read.jdbc(url=in_url_connect, table=in_table, properties=properties)
    return df


def bigqueryReadTable(in_table,in_project):
    df = spark.read.format('bigquery') \
        .option('project', in_project)\
        .option('table', in_table) \
        .load()
    return df


def hiveWriteTable(sampledf,out_db,out_table,out_mode):
    sampledf.write.mode(out_mode).saveAsTable("{}.{}".format(out_db,out_table))


def postgresWriteTable(sampledf,out_url_connect, out_user, out_password, out_driver,out_mode,out_table):
    properties = {"user": out_user, "password": out_password, "driver": out_driver}
    print("----------->",in_table,table)
    sampledf.write.option('driver', out_driver).jdbc(out_url_connect, out_table, out_mode, properties)


def bigqueryWriteTable(sampledf,out_table):
    sampledf.write.format('bigquery') \
        .option('table', out_table) \
        .save()


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
    outengine = obj.outInfo["outengine"]
    out_url_connect = obj.outInfo["url_connect"]
    out_mode = obj.outInfo["mode"]
    out_db = obj.outInfo["outdb"]
    out_table = obj.outInfo["outtable"]
    out_username = obj.outInfo["user"]
    out_password = obj.outInfo["password"]
    errorengine = obj.error_log["errorengine"]
    error_driver =  obj.error_log["driver"]
    errordb = obj.error_log["errordb"]
    errortable = obj.error_log["table"]
    errormode = obj.error_log["mode"]
    error_url_connect = obj.error_log["url_connect"]
    error_user = obj.error_log["user"]
    error_password = obj.error_log["password"]
    try :
        print(0/0)
        print("table : ------------>",obj.table)
        df = spark.createDataFrame(spark.sparkContext.emptyRDD(),StructType([]))
        if inengine == "postgres":
            df = postgresReadTable(in_url_connect, in_user, in_password, in_driver, in_table)
        if inengine == "hive":
            df = hiveReadTable(in_db,in_table)
        if inengine == "bigquery":
            errorproject = obj.error_log["project"]
            in_bucket = obj.inputInfo["bucket"]
            spark.conf.set('temporaryGcsBucket', in_bucket)
            in_project = obj.inputInfo["project"]
            df = bigqueryReadTable(in_table,in_project)


        sampledf = df.sample(fraction)
        while sampledf.count() == 0 or sampledf.count() > (df.count() * fraction):
            sampledf = df.sample(fraction)
        cols = df.dtypes
        for colm in cols:
            basecol = obj.maskedcolumns
            if colm[0] in mstcols and colm[1] == "int":
                print("int column",colm[0])
                sampledf = sampledf.withColumn(basecol[colm[0]],
                                            when(hash(sampledf[colm[0]]) < 0,
                                                hash(sampledf[colm[0]]) * -1).otherwise(hash(sampledf[colm[0]])))
                print(sampledf.show())
            if colm[0] in mstcols and colm[1] == "string":
                sampledf = sampledf.withColumn(basecol[colm[0]], sha2((sampledf[colm[0]]), 256))
        if(outengine=='postgres'):
            postgresWriteTable(sampledf,in_url_connect, in_user, in_password, in_driver,out_mode,out_table)
        if (outengine == 'hive'):
            hiveWriteTable(sampledf,out_db,out_table,out_mode)
        if (outengine == 'bigquery'):
            bigqueryWriteTable(sampledf,out_table)

    except Exception as jobException:
        appid = sc._jsc.sc().applicationId()
        appname = sc._jsc.sc().appName()
        print(appid, appname)
        errorTime = datetime.now().timestamp()
        # print(k.show())
        startTime = spark.sql(
            "select from_unixtime(unix_timestamp(current_timestamp,'yyyy-MM-dd HH:mm:ss'),'dd-MM-yyyy h:mm:ss') s").collect()[
            0]['s']
        errorList = sc.parallelize([(appid, appname, startTime, jobException)], 1)
        schema = StructType([
            StructField("appid", StringType(), True),
            StructField("appname", StringType(), True),
            StructField("errortime", StringType(), True),
            StructField("description", StringType(), True)])
        errordf = spark.createDataFrame(errorList, schema=schema)
        errordf = errordf.withColumn("errortime", to_timestamp(errordf.errortime, 'MM-dd-yyyy HH:mm:ss'))

        if(outengine=='postgres'):
            postgresWriteTable(errordf,error_url_connect, error_user, error_password, error_driver,errormode,errortable)
        if (outengine == 'hive'):
            hiveWriteTable(errordf,errordb,errortable,errormode)
        if (outengine == 'bigquery'):
            bigqueryWriteTable(errordf,errortable)
