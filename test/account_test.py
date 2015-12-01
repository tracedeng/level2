# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import socket
import common_pb2
import package

import calculus.wrapper.log as log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from branch_socket import receive_from_sock
from branch_socket import send_to_sock


def register_request_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 3
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.register_request
    material.numbers = "18688982240"
    material.password = "123456"
    material.password_md5 = "c56d0e9a7ccec67b4ea131655038d604"

    pack_send_receive(req)


def login_request_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 2
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.login_request
    material.numbers = "18688982240"
    material.password_md5 = "7a9e4b5025a8adc7d3208fd66806d685"

    pack_send_receive(req)


def change_password_request_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 4
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.change_password_request
    material.numbers = "18688982240"
    material.password = "654321"
    material.password_md5 = "7a9e4b5025a8adc7d3208fd66806d685"

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


# def batch_test_create():
#     import time
#     begin = 15100001111
#     last = 15100101111
#     start = time.clock()
#     for i in xrange(begin, last):
#         consumer_create_test(str(i))
#     end = time.clock()
#     g_log.debug("create %s consumer, cost %s cpu time", last - begin, end-start)
#
#
# def batch_test_retrieve():
#     import time
#     begin = 15100001111
#     last = 15100101111
#     start = time.clock()
#     for i in xrange(begin, last):
#         consumer_retrieve_test(str(i))
#     end = time.clock()
#     g_log.debug("retrieve %s consumer, cost %s cpu time", last - begin, end-start)
#     print("retrieve %s consumer, cost %s cpu time" % (last - begin, end-start))


if "__main__" == __name__:
    # register_request_test()
    login_request_test()
    # change_password_request_test()
    pass