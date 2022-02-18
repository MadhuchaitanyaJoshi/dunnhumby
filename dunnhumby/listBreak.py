import math
ls = [i for i in range(1,61)]
size = 4
cnt = math.ceil(len(ls)/size)
print(ls)
print(cnt)
start=0
end=size
for i in range(0,cnt):
    print(start,end)
    sub = ls[start:end+1]
    print(sub)
    start = end+1
    end = end+size