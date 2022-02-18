d = {1:1,2:2,3:3,4:4}


for i in d.copy():
    if i==1:
        del d[i]
        print(d)
print(d)


