# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import socket
import common_pb2
import package
from branch_socket import receive_from_sock

address = ('127.0.0.1', 9527)


def consumer_create_test():
    global address
    req = common_pb2.Request()
    req.head.cmd = 100
    req.head.seq = 2
    req.head.phone_number = "18688982240"
    # req.consumer_create_request.phone_number = "18688982240"
    req.consumer_create_request.material.phone_number = "18688982240"
    req.consumer_create_request.material.nickname = "tracedeng"
    req.consumer_create_request.material.sexy = 1

    request = package.serial_pb(req)

    sock = socket.socket(type=socket.SOCK_DGRAM)
    sock.connect(address)
    print('Sending %s bytes to %s:%s' % ((len(request), ) + address))
    print('%s' % req)
    sock.send(request)

    result = receive_from_sock(sock)
    if result == 0:
        return 0
    response, _ = result
    print('receive %s bytes from %s:%s' % ((len(response), ) + _))
    res = common_pb2.Response()
    res.ParseFromString(response[6:-2])
    print('%s' % res)

if "__main__" == __name__:
    consumer_create_test()