# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import socket
import common_pb2
import package

import calculus.wrapper.log as log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from branch_socket import receive_from_sock
from branch_socket import send_to_sock


def platform_update_parameters_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 501
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.platform_update_parameters_request
    material.numbers = numbers
    material.bond = 1000
    material.balance_ratio = 100
    material.merchant_identity = "562c7ad6494ac55faf750798"
    material.manager = "118688982240"

    pack_send_receive(req)


def business_parameters_retrieve_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 402
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.business_parameters_retrieve_request
    material.numbers = numbers
    material.merchant_identity = "562c7ad6494ac55faf750798"

    pack_send_receive(req)


def consumption_ratio_update_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 404
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.consumption_ratio_update_request
    material.numbers = numbers
    material.merchant_identity = "562c7ad6494ac55faf750798"
    material.consumption_ratio = 100

    pack_send_receive(req)


def parameters_record_retrieve_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 406
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.parameters_record_retrieve_request
    material.numbers = numbers
    material.merchant_identity = "562c7ad6494ac55faf750798"

    pack_send_receive(req)


def merchant_recharge_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 407
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.merchant_recharge_request
    material.numbers = numbers
    material.merchant_identity = "562c7ad6494ac55faf750798"
    material.money = 9834

    pack_send_receive(req)


def recharge_record_retrieve_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 408
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.merchant_recharge_record_request
    material.numbers = numbers
    # material.merchant_identity = "562c7ad6494ac55faf750798"

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

#
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
    # platform_update_parameters_test()
    # business_parameters_retrieve_test()
    # consumption_ratio_update_test()
    # business_parameters_retrieve_test()
    # parameters_record_retrieve_test()
    # merchant_recharge_test()
    recharge_record_retrieve_test()
    pass