from dask.distributed import Client, progress
import time
import random
import dask

client = Client('10.65.18.83:8786')
client.get_versions(check=True)
client

def inc(x):
    time.sleep(random.random())
    return x + 1
def dec(x):
    time.sleep(random.random())
    return x - 1
def add(x, y):
    time.sleep(random.random())
    return x + y
inc = dask.delayed(inc)
dec = dask.delayed(dec)
add = dask.delayed(add)
x = inc(1)
y = dec(2)
z = add(x, y)
result = z.compute()
print(result)