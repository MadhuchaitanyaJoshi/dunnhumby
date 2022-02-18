import pandas as pd
df = pd.read_csv("C:/data/backlogjan.csv")
df = df.loc[:,["campaign_id","manual_date","fis_week_id"]]
df = df.applymap(str)
df['campaign_id'] = df[['manual_date','campaign_id','fis_week_id']].groupby(['manual_date','fis_week_id'])['campaign_id'].transform(lambda x: ','.join(x))
df = df.drop_duplicates()
pls = df.values.tolist()
jobList = {}
mk = {1:'a',2:'v'}
jobListLen = {}
for row in pls:
    p = "-".join(row[1].split("-")[::-1])
    jobList[p+","+row[2]] = row[0].split(",")
    jobListLen[p + "," + row[2]] = len(row[0].split(","))
print(jobList)
print(jobListLen)
k = 0
for i in jobListLen:
    k = k+jobListLen[i]
print(k)

mk.update(jobList)
print(mk)