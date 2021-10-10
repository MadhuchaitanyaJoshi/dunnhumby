import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
conf = SparkConf().setAppName(value="datagenartion")
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")
arg = sys.argv
print(sc.applicationId,sc.appName)
try:
    with open(arg[2],"r") as mstcolumns:
        mstcols = mstcolumns.read().split(",")
    with open(arg[1],"r") as config:
        conf = json.load(config)
        # print(conf["retail"]["cust"])
    for db in conf:
        # print(db)
        for table in conf[db]:
            if table!="outdb":
                df = spark.sql("select * from {}.{}".format(db,table))
                sampledf = df.sample(conf[db][table]["fraction"])
                # print(df.show(10,False),df.printSchema())
                # print(sampledf.show(20,False))
                # print(conf[db]["outdb"])
                while(sampledf.count()==0 or sampledf.count()>(df.count()*conf[db][table]["fraction"])):
                    sampledf = df.sample(conf[db][table]["fraction"])
                cols = df.dtypes
                for colm in cols:
                    basecol = conf[db][table]["maskcolumns"]
                    # print("************",basecol,colm)
                    if(colm[0] in mstcols and colm[1]=="int"):
                        # print("----------->",basecol[colm[0]])
                        sampledf = sampledf.withColumn(conf[db][table]["maskcolumns"][colm[0]], when(hash(sampledf[colm[0]])<0,hash(sampledf[colm[0]])*-1 ).otherwise(hash(sampledf[colm[0]])))
                    if(colm[0] in mstcols and colm[1]=="string"):
                        sampledf = sampledf.withColumn(basecol[colm[0]], sha2((sampledf[colm[0]]), 256))
                print("------------>",sampledf.show(20,False))
                sampledf.write.mode("append").saveAsTable("{}.{}".format(conf[db]["outdb"],conf[db][table]["outtable"]))
except Exception as e:
    print(e)
    applicationId = sc.applicationId
    appname = sc.appName
    errorList = sc.parallelize([applicationId,appname,current_timestamp(),e])
    errordf = spark.createDataFrame(errorList,["application_id","aplication_name","errorTime","description"])
    errordf.write.format('jdbc').options(
        url='jdbc:mysql://localhost/{}'.format("retail"),
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='{}'.format("error_log"),
        user='root',
        password='root').option("useSSL", "false").mode('append').save()