from concurrent.futures import ThreadPoolExecutor
def gfg(a,b):
    print(a+b)
with ThreadPoolExecutor(max_workers=1) as executor:
    executor.submit(gfg, 323, 1235)
    print()