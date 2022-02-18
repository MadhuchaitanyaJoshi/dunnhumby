ls = [2, 1, 4, 2, 21, 34, 45, 111, 29]

def sort1(lst):
    i=0
    j=i+1
    while(i<len(lst) and j<len(lst)):
        if(lst[i]<lst[j] and j+1<len(lst)):

            print("-----------",i,j)
        else:
            print("-----------", i, j)
            k = lst[i]
            lst[i] = lst[j]
            lst[j]=k
            print(lst)
            i=i+1
            j=i+1

    print(lst)

sort1(ls)
