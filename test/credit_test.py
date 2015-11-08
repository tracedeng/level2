# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import socket
import common_pb2
import package

import calculus.wrapper.log as log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from branch_socket import receive_from_sock
from branch_socket import send_to_sock


def consumption_create_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 301
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.consumption_create_request
    material.numbers = numbers
    material.merchant_identity = "562c7ad6494ac55faf750798"
    material.sums = 180

    pack_send_receive(req)


def credit_free_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 305
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.credit_free_request
    material.numbers = '18688988888'
    material.manager_numbers = numbers
    material.merchant_identity = "562c7ad6494ac55faf750798"
    material.credit = 18000

    pack_send_receive(req)


def merchant_credit_retrieve_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 302
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.merchant_credit_retrieve_request
    material.numbers = numbers
    material.merchant_identity = "562c7ad6494ac55faf750798"

    pack_send_receive(req)


def confirm_consumption_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 303
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.confirm_consumption_request
    material.numbers = "18688982240"
    material.manager_numbers = numbers
    material.merchant_identity = "562c7ad6494ac55faf750798"
    material.credit_identity = "56332c20e91a571b6000b2f5"
    material.credit = 18000

    pack_send_receive(req)


def consumer_credit_retrieve_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 306
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.consumer_credit_retrieve_request
    material.numbers = numbers

    pack_send_receive(req)


def consume_credit_test(numbers='18688982240'):
    req = common_pb2.Request()
    req.head.cmd = 307
    req.head.seq = 2
    req.head.numbers = numbers
    material = req.consume_credit_request
    material.numbers = numbers
    material.credit_identity = "56332c20e91a571b6000b2f5"
    material.credit = 1000

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
    
    
if "__main__" == __name__:
    # consumption_create_test()
    # credit_free_test()
    # consumption_update_test()
    # consumption_retrieve_test()
    # batch_test_create()
    # batch_test_retrieve()
    # merchant_credit_retrieve_test()
    # confirm_consumption_test()
    # consumer_credit_retrieve_test()
    consume_credit_test()
    pass