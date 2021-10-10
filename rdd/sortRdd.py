from pyspark import SparkContext
sc = SparkContext()

s = sc.textFile("C:/Users/mjoshi/PycharmProjects/pythonProject/data/student.txt")
wc = s.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
print(wc.sortBy(lambda x:x[1],ascending=False,numPartitions=1).collect())
#print(wc.map())
print(wc.sortByKey().collect())
