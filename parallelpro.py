import datetime
import time
import os
from concurrent.futures import ThreadPoolExecutor
lst = [1,2,3,4,5,6,7,9,10]
def sqr(a):
    print("stsrted:---------",os.getpid())
    print(a*a)
    time.sleep(1)
    print("ended:---------",os.getpid())
with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(sqr,lst)
        print(datetime.datetime.now())
print("completed the proces")
# for i in lst:
#     sqr(i)
#     print(datetime.datetime.now())
