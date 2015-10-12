# -*- coding: utf-8 -*-

__author__ = 'tracedeng'


from gevent import socket
from gevent import monkey
import gevent
import urllib2


def f(url):
    print('GET: %s' % url)
    resp = urllib2.urlopen(url)
    data = resp.read()
    print('%d bytes received from %s.' % (len(data), url))


def foo():
    print "hello"


if "__main__" == __name__:
    monkey.patch_all()
    g = gevent.spawn(f, 'https://www.baidu.com')
    print "hello world"
    gevent.joinall([
        g,
        gevent.spawn(f, 'https://www.python.org'),
        gevent.spawn(f, 'https://www.yahoo.com/'),
        gevent.spawn(f, 'https://github.com/'),
    ], 0)
    print "abc"
    gevent.wait()
    # print socket.
