# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

# import socket
import ConfigParser
import struct

import gevent
from gevent import Timeout

import common_pb2

import calculus.wrapper.log as log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能

__all__ = ['Master']


class Master():
    TIMEOUT = 2000

    def __init__(self, *args, **kwargs):
        """
        解析配置文件，读取超时时间
        :param args: None
        :param kwargs: {"config": config}
        :return: None
        """
        config = kwargs["config"]
        try:
            expire = config.get("master", "timeout")
        except ConfigParser.NoSectionError as e:
            g_log.warning("config file miss section master, timeout 2000ms")
            expire = Master.TIMEOUT
        except ConfigParser.NoOptionError as e:
            g_log.warning("config file miss %s:%s", e.section, e.option)
            expire = Master.TIMEOUT
        g_log.debug("master timeout %sms", expire)
        self.expire = float(expire) / 1000  # 转换成秒


    def _timeout_response(self, cmd, seq):
        response = common_pb2.Response()
        response.head.cmd = cmd
        response.head.seq = seq
        response.head.retcode = 1
        response.head.message = "register timeout"
        response_serial = response.SerializeToString()
        return response_serial

    def enter(self, data):
        """
        主线处理入口，启动定时器，解析请求，返回业务逻辑处理结果
        :param data: 请求包
        :return: 返回处理结果，(errcode, errmsg | data)
        errcode: 0-成功，1-超时，2:－其它错误
        """
        timeout = gevent.Timeout(self.expire)
        timeout.start()
        try:
            g_log.debug("%s: begin service logic ...", gevent.getcurrent())
            request = common_pb2.Request()
            request.ParseFromString(data[6:-2])
            g_log.debug(request)
            head = request.head
            # g_log.debug("cmd:%s", head.cmd)
            # g_log.debug("seq:%s", head.seq)
            if head.cmd == 1:
                # 注册
                register = request.register
                # g_log.debug(register)
            response = common_pb2.Response()
            response.head.cmd = head.cmd
            response.head.seq = head.seq
            response.head.retcode = 1
            response.head.message = "register succeed"
            g_log.debug(response)
            # g_log.debug("retcode = %s", response.head.retcode)
            response_serial = response.SerializeToString()
            # gevent.sleep(2)

            length = len(response_serial) + 4
            f = "!I%ss" % length
            message = struct.pack(f, length, "{{%s}}" % response_serial)
            g_log.debug("%s: end service logic ...", gevent.getcurrent())
            return message
        except Timeout as e:
            # 超时，关闭定时器
            # timeout.cancel()
            # 返回超时结果
            return self._timeout_response(head.cmd, head.seq)
        finally:
            timeout.cancel()