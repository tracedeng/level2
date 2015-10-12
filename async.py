# -*- coding: utf-8 -*-

__author__ = 'tracedeng'

import gevent
import random


def fetch(pid):
    gevent.sleep(random.randint(0, 2) * 1)
    print('Task %s done' % pid)


def synchronous():
    for i in range(1, 10):
        fetch(i)


def asynchronous():
    threads = []
    for i in range(1, 10):
        threads.append(gevent.spawn(fetch, i))
    gevent.joinall(threads)


if "__main__" == __name__:
    print('Synchronous:')
    synchronous()

    print('Asynchronous:')
    asynchronous()