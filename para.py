__author__ = 'tracedeng'

from multiprocessing import Pool
from datetime import datetime
import time


def echo(i):
    time.sleep(1)
    return i


z = 9


def foo():
    print "hello child"
    global z
    z = 7
    print z
# Non Deterministic Process Pool

p = Pool(10)
print datetime.now()
run2 = p.map(echo, range(10))
print run2
print datetime.now()

print datetime.now()
run3 = p.map_async(echo, range(10))
print run3.get()
print datetime.now()

print datetime.now()
run4 = [a for a in p.imap(echo, xrange(10))]
print run4
print datetime.now()

print datetime.now()
run1 = [a for a in p.imap_unordered(echo, xrange(10))]
print run1
print datetime.now()

print z
z = 8
p.apply(foo)
print z
