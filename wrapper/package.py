# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import struct
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能，将这行代码放在import自定义模块最前面
import common_pb2


def check_message(message):
    """
    检查包有效性
    :param message: 包
    :return: 1/成功，0/失败
    """
    if len(message) < 8:   # 4Byte长度 ＋ {{ + PB + }}，长度＝length({{ + PB + }})
        g_log("illegal package, not enough length")
        return 0
    (length, stx) = struct.unpack('!I2s', message[0:6])
    if stx != "{{":
        g_log.warning("illegal package, illegal stx(%s)", stx)
        return 0

    if len(message) != (length + 4):
        g_log.warning("illegal package, illegal length")
        return 0

    etx = message[-2:]
    if etx != "}}":
        g_log.warning("illegal package, illegal etx(%s)", etx)
        return 0

    return 1


def timeout_response(cmd, seq):
    """
    组装超时包
    :param cmd: 命令号
    :param seq: 序号
    :return:
    """
    response = common_pb2.Response()
    response.head.cmd = cmd
    response.head.seq = seq
    response.head.code = 1
    response.head.message = "timeout"
    return response


def exception_response(cmd, seq):
    """
    组装异常包
    :param cmd: 命令号
    :param seq: 序号
    :return:
    """
    response = common_pb2.Response()
    response.head.cmd = cmd
    response.head.seq = seq
    response.head.code = 1
    response.head.message = "server exception, retry later"
    return response


def serial_pb(pb):
    """
    序列化pb包
    :param pb:
    :return:
    """
    pb_serial = pb.SerializeToString()
    length = len(pb_serial) + 4
    f = "!I%ss" % length
    message = struct.pack(f, length, "{{%s}}" % pb_serial)
    return message