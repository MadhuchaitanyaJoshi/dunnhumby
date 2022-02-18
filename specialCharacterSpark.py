from pyspark import SparkContext
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql.functions import *
from pyspark.sql.types import *
sc = SparkContext()
spark = SparkSession(sc)
def duplicateColumns(df):
    f = df.columns
    dup = {}
    f.sort()
    print(f)
    prev_col = f[0]
    for i in f:
        if i not in dup:
            dup[i] = 1
        else:
            dup[i] = dup[i] + 1
    print(dup)
    for i in range(0, len(f)):
        if (i > 0 and prev_col == f[i]):
            print(i, f[i], prev_col)
            prev_col = f[i]
            f[i] = "second." + f[i]
        elif (dup[f[i]] == 1):
            prev_col = f[i]
            f[i] = "" + f[i]
        else:
            prev_col = f[i]
            f[i] = "first." + f[i]
    print(f)
    print(dup)
    df = df.select(*f)
    print("----------------------------------")
    print(df.show(5,False))
    print("----------------------------------")

    return df

def checkRegex(df, cl):
    #return df.withColumn("corruptColumn",when(df[cl].rlike('^[a-zA-Z0-9]*$',False)))
    df = df.withColumn('matched',
                       when(df[cl].rlike('^[a-zA-Z0-9]*$'),concat_ws("",df["matched"],lit(""))).when(df[cl].isNull(),concat_ws("",df["matched"],lit(""))).otherwise(concat_ws(",",df["matched"],lit(cl))))
    return df

df = spark.read.csv("C:/dev/data/characterFind.csv",header=True).withColumn("matched",lit(None).cast(StringType()))
df = duplicateColumns(df)
print(df.show())
ls = [cols for cols in df.columns if ("code" in cols or "id" in cols)]
cases = reduce(checkRegex, ls, df)
print(cases.show(20,False))
flt = cases.filter(cases["matched"].contains("subject_code"))
print(flt.show(20,False))
#
#
#
#f = ["madhu","madhu","albert","albert","roger"]
# dup = {}
# f.sort()
# print(f)
# prev_col = f[0]
# for i in f:
#     if i not in dup:
#         dup[i]=1
#     else:
#         dup[i] = dup[i]+1
# print(dup)
# for i in range(0, len(f)):
#     if (i > 0 and prev_col == f[i]):
#         print(i, f[i], prev_col)
#         prev_col = f[i]
#         f[i] = "second." + f[i]
#     elif(dup[f[i]]==1):
#         prev_col = f[i]
#         f[i] = "" + f[i]
#     else:
#         prev_col = f[i]
#         f[i] = "first." + f[i]
# print(f)
# print(dup)