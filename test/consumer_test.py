# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import socket
import common_pb2
import package
from branch_socket import receive_from_sock
from branch_socket import send_to_sock

address = ('127.0.0.1', 9527)


def consumer_create_test(phone_number='18688982240'):
    global address
    req = common_pb2.Request()
    req.head.cmd = 100
    req.head.seq = 2
    req.head.phone_number = phone_number
    # req.consumer_create_request.phone_number = "18688982240"
    material = req.consumer_create_request.material
    req.consumer_create_request.material.phone_number = phone_number
    req.consumer_create_request.material.nickname = "tracedengtracedengtracedengtracedengtracedengtracedeng"
    req.consumer_create_request.material.sexy = 1
    material.introduce = "i introduce myself"
    material.email = "18688982240@qq.com"
    material.age = 18
    material.country = "china"
    material.location = "where am i"

    pack_send_receive(req)


def consumer_retrieve_test():
    global address
    req = common_pb2.Request()
    req.head.cmd = 101
    req.head.seq = 2
    req.head.phone_number = "18688982243"
    req.consumer_retrieve_request.phone_number = "18688982243"

    pack_send_receive(req)


def consumer_update_test():
    global address
    req = common_pb2.Request()
    req.head.cmd = 103
    req.head.seq = 2
    req.head.phone_number = "18688982243"
    req.consumer_update_request.phone_number = "18688982243"
    req.consumer_update_request.material.nickname = "dudu"
    req.consumer_update_request.material.age = 1000

    pack_send_receive(req)


def consumer_delete_test():
    global address
    req = common_pb2.Request()
    req.head.cmd = 104
    req.head.seq = 2
    req.head.phone_number = "18688982243"
    req.consumer_delete_request.phone_number = "18688982243"

    pack_send_receive(req)


def pack_send_receive(req):
    global address
    request = package.serial_pb(req)

    # send_to_address(request, address)
    sock = socket.socket(type=socket.SOCK_DGRAM)
    sock.connect(address)
    # print('Sending %s bytes to %s:%s' % ((len(request), ) + address))
    # print('%s' % req)
    # sock.send(request)
    send_to_sock(sock, request)

    result = receive_from_sock(sock)
    if result == 0:
        return 0
    response, _ = result
    # print('receive %s bytes from %s:%s' % ((len(response), ) + _))
    res = common_pb2.Response()
    res.ParseFromString(response[6:-2])
    # print('%s' % res)

if "__main__" == __name__:
    # consumer_delete_test()
    import time
    start = time.clock()
    for i in xrange(15100001111, 15100101111):
        consumer_create_test(str(i))
    end = time.clock()
    print end-start
    # consumer_retrieve_test()
    # consumer_update_test()
    # consumer_retrieve_test()