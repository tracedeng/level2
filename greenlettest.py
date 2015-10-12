# -*- coding: utf-8 -*-

__author__ = 'tracedeng'

from greenlet import greenlet


def foo2(key):
    global y
    print "foo2"
    print key
    # print greenlet.getcurrent().parent
    y = gr1.switch(9)
    return y


def foo1(key):
    # print greenlet.getcurrent().dead
    global z
    print "foo1"
    print key
    # print greenlet.getcurrent().parent
    z = gr2.switch(9528)
    print "guoge"


if "__main__" == __name__:
    y = 7
    z = 7
    # print greenlet.getcurrent()
    gr1 = greenlet(foo1)
    gr2 = greenlet(foo2)
    # print gr1.run
    print "hello world"
    gr1.switch(9527)
    # gr2.switch(99)
    print y
    print z
    print(gr1.dead)
    print(gr2.dead)
    k=gr2.switch(6)
    # print(gr1.dead)
    # print(gr2.dead)
    print(k)
