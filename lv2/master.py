# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import socket
import google.protobuf

import calculus.wrapper.log as log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能

__all__ = ['Master']

class Master():

    def __init__(self):
        pass

    def enter(self, data):
        print("i am here")

        pass

