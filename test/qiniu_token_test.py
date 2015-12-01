# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import socket
import common_pb2
import package

import calculus.wrapper.log as log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from branch_socket import receive_from_sock
from branch_socket import send_to_sock


def upload_token_avatar_test(numbers='118688982240'):
    req = common_pb2.Request()
    req.head.cmd = 601
    req.head.seq = 2
    req.head.numbers = numbers
    body = req.upload_token_request

    body.numbers = numbers
    # 上传的资源类型，用户头像c_avatar, 商家logo m_logo, 商家活动海报ma_poster
    body.resource_kind = "c_avatar"
    body.debug = "debug"

    pack_send_receive(req)


def upload_token_logo_test(numbers='218688982240'):
    req = common_pb2.Request()
    req.head.cmd = 601
    req.head.seq = 2
    req.head.numbers = numbers
    body = req.upload_token_request

    body.numbers = numbers
    # 上传的资源类型，用户头像c_avatar, 商家logo m_logo, 商家活动海报ma_poster
    body.resource_kind = "m_logo"
    body.merchant_identity = "562c7ad6494ac55faf750798"
    body.debug = "debug"

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
    # upload_token_avatar_test()
    upload_token_logo_test()
    pass