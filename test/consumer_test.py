# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import socket
import common_pb2
import package

import calculus.wrapper.log as log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from branch_socket import receive_from_sock
from branch_socket import send_to_sock


def consumer_create_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 101
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.consumer_create_request.material
    req.consumer_create_request.material.numbers = numbers
    req.consumer_create_request.material.nickname = "tracedengtracedengtracedengtracedengtracedengtracedeng"
    req.consumer_create_request.material.sexy = "male"
    material.introduce = "i introduce myself"
    material.email = "18688982240@qq.com"
    material.age = 18
    material.country = "china"
    material.location = "where am i"

    pack_send_receive(req)


def consumer_retrieve_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 102
    req.head.seq = 2
    req.head.numbers = numbers
    req.consumer_retrieve_request.numbers = numbers

    pack_send_receive(req)


def consumer_update_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 104
    req.head.seq = 2
    req.head.numbers = numbers
    req.consumer_update_request.numbers = numbers
    req.consumer_update_request.material.nickname = "dudu"
    req.consumer_update_request.material.age = 1000

    pack_send_receive(req)


def consumer_delete_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 105
    req.head.seq = 2
    req.head.numbers = numbers
    req.consumer_delete_request.numbers = numbers

    pack_send_receive(req)


def pack_send_receive(req):
    address = ('127.0.0.1', 9527)
    request = package.serial_pb(req)

    # 发包
    sock = socket.socket(type=socket.SOCK_DGRAM)
    sock.connect(address)
    g_log.debug("Sending %s bytes to %s", len(request), address)
    g_log.debug("%s", req)
    send_to_sock(sock, request)

    # 收包
    result = receive_from_sock(sock)
    if result == 0:
        return 0
    response, _ = result
    g_log.debug('receive %s bytes from %s', len(response), _)

    res = common_pb2.Response()
    res.ParseFromString(response[6:-2])
    g_log.debug('%s', res)


def batch_test_create():
    import time
    begin = 15100001111
    last = 15100101111
    start = time.clock()
    for i in xrange(begin, last):
        consumer_create_test(str(i))
    end = time.clock()
    g_log.debug("create %s consumer, cost %s cpu time", last - begin, end-start)


def batch_test_retrieve():
    import time
    begin = 15100001111
    last = 15100101111
    start = time.clock()
    for i in xrange(begin, last):
        consumer_retrieve_test(str(i))
    end = time.clock()
    g_log.debug("retrieve %s consumer, cost %s cpu time", last - begin, end-start)
    print("retrieve %s consumer, cost %s cpu time" % (last - begin, end-start))


if "__main__" == __name__:
    # consumer_delete_test()
    consumer_create_test(numbers="18688988888")
    # consumer_retrieve_test()
    # consumer_update_test()
    # consumer_retrieve_test()
    # batch_test_create()
    # batch_test_retrieve()
    pass