# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import socket
import google.protobuf
import gevent

import calculus.wrapper.log as log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能

__all__ = ['Master']


class Master():

    def __init__(self):
        self.coroutine_name = "abc"
        pass

    def enter(self, data):
        """
        主线处理入口，返回业务逻辑处理结果
        :param data: 请求包
        :return: 返回处理结果，数据按照协议封装，直接返回
        """
        # print("i am here")
        g_log.debug("%s: begin service logic ...", gevent.getcurrent())
        g_log.debug("%s: end service logic ...", gevent.getcurrent())
        return data
