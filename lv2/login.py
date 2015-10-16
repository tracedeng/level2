# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import common_pb2
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
import branch_socket


class Login():
    """
    注册登录模块，命令号<100
    request：请求包解析后的pb格式
    """
    def __init__(self, request):
        self.request = request
        self.head = request.head
        self.cmd = request.head.cmd
        self.seq = request.head.seq


    def enter(self):
        """
        处理具体业务
        :return: 0/不回包给前端，pb/正确返回，timeout/超时
        """
        if self.cmd == 1:
            # 注册

            # 模拟旁路
            vip = common_pb2.Request()
            vip.head.cmd = 1000
            vip.head.seq = 10
            vip.head.phone_number = self.head.phone_number

            response = branch_socket.send_to_branch("vip", vip)
            if response == 0:
                # 旁路错误逻辑，用户根据业务情况自己决定返回结果
                pass
            elif response == "timeout":
                # 旁路超时逻辑，用户根据业务情况自己决定返回结果
                # return "timeout"
                # g_log.debug("vip timeoutttttttt")
                pass
            else:
                g_log.debug("branch vip check ok")
                # g_log.debug("get response from branch vip")
                # g_log.debug("%s", response)

            # 返回结果
            response = common_pb2.Response()
            response.head.cmd = self.head.cmd
            response.head.seq = self.head.seq
            response.head.code = 1
            response.head.message = "register succeed"
            g_log.debug("%s", response)
            return response
        else:
            # 错误的命令
            return 0


def enter(request):
    """
    登录模块入口
    :param request: 解析后的pb格式
    :return: 0/不回包给前端，pb/正确返回，timeout/超时
    """
    try:
        login = Login(request)
        return login.enter()
    except Exception as e:
        g_log.error("%s", e)
        return 0