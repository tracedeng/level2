# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import socket
import common_pb2
import package

import calculus.wrapper.log as log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from branch_socket import receive_from_sock
from branch_socket import send_to_sock


def merchant_create_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 201
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.merchant_create_request.material
    req.merchant_create_request.material.numbers = numbers
    req.merchant_create_request.material.name = "StarBucks"
    material.introduce = "i introduce myself"
    material.email = "18688982240@qq.com"
    # material.age = 18
    material.country = "china"
    material.location = "where am i"

    pack_send_receive(req)


def merchant_retrieve_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 202
    req.head.seq = 2
    req.head.numbers = numbers
    req.merchant_retrieve_request.numbers = numbers
    req.merchant_retrieve_request.merchant_identity = "56273ddf4e7915048ee91f48"

    pack_send_receive(req)


def merchant_update_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 204
    req.head.seq = 2
    req.head.numbers = numbers
    req.merchant_update_request.numbers = numbers
    req.merchant_update_request.material.nickname = "dudu"
    req.merchant_update_request.material.age = 1000

    pack_send_receive(req)


def merchant_delete_manager_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 209
    req.head.seq = 2
    req.head.numbers = numbers
    req.merchant_delete_request.numbers = numbers

    pack_send_receive(req)


def merchant_create_manager_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 207
    req.head.seq = 2
    req.head.numbers = numbers
    req.merchant_create_manager_request.numbers = numbers
    req.merchant_create_manager_request.merchant_identity = "56273ddf4e7915048ee91f48"
    req.merchant_create_manager_request.manager_numbers = "118688982241"

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
    # return res


def batch_test_create():
    import time
    begin = 15100001111
    last = 15100101111
    start = time.clock()
    for i in xrange(begin, last):
        merchant_create_test(str(i))
    end = time.clock()
    g_log.debug("create %s merchant, cost %s cpu time", last - begin, end-start)


def batch_test_retrieve():
    import time
    begin = 15100001111
    last = 15100101111
    start = time.clock()
    for i in xrange(begin, last):
        merchant_retrieve_test(str(i))
    end = time.clock()
    g_log.debug("retrieve %s merchant, cost %s cpu time", last - begin, end-start)
    print("retrieve %s merchant, cost %s cpu time" % (last - begin, end-start))


if "__main__" == __name__:
    # merchant_delete_test()
    merchant_create_test()
    # merchant_retrieve_test()
    # merchant_update_test()
    # merchant_retrieve_test()
    # batch_test_create()
    # batch_test_retrieve()
    # merchant_create_manager_test()
    pass