# -*- coding: utf-8 -*-

__author__ = 'tracedeng'


def coroute():
    """

    :rtype : None
    """
    print("hello world")
    print "hello world"
    print "中文"
    print u"中文"


def odd():
    print 'step 1'
    yield 1
    print 'step 2'
    yield 3
    print 'step 3'
    yield 5


import time
import sys


# 生产者
def produce(l):
    i = 0
    while 1:
        if i < 5:
            l.append(i)
            yield i
            i += 1
            time.sleep(1)
        else:
            return


# 消费者
def consume(l):
    p = produce(l)
    while 1:
        try:
            p.next()
            while len(l) > 0:
                print l.pop()
        except StopIteration:
            sys.exit(0)


if '__main__' == __name__:
    print sys.path
    print sys.modules
