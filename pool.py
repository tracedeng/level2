__author__ = 'tracedeng'

from gevent import monkey
monkey.patch_all()

import urllib2
from gevent.pool import Pool
import multiprocessing.sharedctypes
import ctypes

def download(url):
    return urllib2.urlopen(url).read()


if __name__ == '__main__':
    urls = ['http://httpbin.org/get'] * 100
    pool = Pool(20)
    print pool.map(download, urls)
