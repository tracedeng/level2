# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import common_pb2
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
import coroutine_manage


class BranchBase():
    """
    旁路负类，具体旁路可继承
    """
    def __init__(self, name):
        self.name = name
        pass

    def enter(self, message):
        """
        收到旁路回包，switch到对应的协程处理，如遇任何异常交给协程超时处理
        :param message: 旁路回包
        :return:
        """
        try:
            request = common_pb2.Request()
            request.ParseFromString(message[6:-2])
            g_log.debug("receive message from branch %s", self.name)
            g_log.debug("%s", request)
            head = request.head
            uuid = head.coroutine_uuid
            coroutine_manage.coroutine_switch(uuid, message, kill_self=False)
        except Exception as e:
            g_log.error("%s", e)