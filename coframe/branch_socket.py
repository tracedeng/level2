# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能

from singleton import singleton
import package
import coroutine_manage


# 单例类，import时路径要一致，否则起不到效果
@singleton
class BranchSocket():
    """
    单例类
    和旁路通信
    branch_sock = {"branch1":{"socket":socket, "address":address}, "branch2":{"socket":socket2, "address":address2}}
    """

    def __init__(self):
        self.branch_sock = {}
        # pass

    def add_branch(self, **kwargs):
        """
        添加旁路
        :param kwargs: {"name":"branch", "socket":socket, "address":"127.0.0.1:9527", "timeout":200}
        :return: 0/失败, 1/成功
        """
        if "name" in kwargs and "socket" in kwargs and "address" in kwargs:
            # TODO 检查socket和address的有效性
            address = kwargs["address"].split(":")
            # g_log.debug("%s", address)
            address = (address[0], int(address[1]))
            # g_log.debug("%s", address)
            self.branch_sock[kwargs["name"]] = {"socket": kwargs["socket"], "address": address,
                                                "timeout": kwargs["timeout"]}
            g_log.debug("add branch %s socket, timeout: %sms, address: %s", kwargs["name"],
                        kwargs["timeout"], kwargs["address"])
            return 1
        else:
            g_log.warning("add branch failed, check input arguments, %s", kwargs)
            return 0

    def send_to_branch(self, name, message):
        """
        发送包到旁路
        :param branch_name: 旁路名称
        :return: 0/失败, 1/成功
        """
        # g_log.debug("%s", self.branch_sock)
        branch = self.branch_sock.get(name)
        if branch:
            sock = branch.get("socket")
            address = branch.get("address")
            if sock and address:
                try:
                    sock.sendto(message, address)
                    g_log.debug("send message to branch %s %s", name, address)
                    return 1
                except Exception as e:
                    g_log.debug("%s", e)
                    g_log.error("send message to %s failed", address)
                    return 0
            else:
                return 0
        else:
            g_log.warning("branch %s not exist", name)
            return 0

    def receive_from_branch(self, name):
        """
        从旁路收包
        :param name: 旁路名称
        :return: 0/失败，(message, address)/成功
        """
        branch = self.branch_sock.get(name)
        if branch:
            sock = branch.get("socket")
            if sock:
                try:
                    message, address = sock.recvfrom(65536)
                    return message, address
                except Exception as e:
                    g_log.debug("%s", e)
                    g_log.error("receive message from branch %s failed", name)
                    return 0
            else:
                return 0
        else:
            g_log.warning("branch %s not exist", name)
            return 0


# 语法糖
def send_to_branch(name, request):
    """
    组pb包，异步发送给旁路，switch到loop，当loop收到旁路回包后，switch到当前协程
    :param name: 旁路名称
    :param request: 发送到旁路的pb格式请求包
    :param timeout: 旁路超时时间，缺省没有超时
    :return: 错误/0，timeout/超时，旁路返回包/正确
    """
    try:
        # 请求包补上协程UUID
        uuid = coroutine_manage.generate_coroutine_uuid()
        if uuid == 0:
            return 0
        # g_log.debug("generate coroutine uuid %s", uuid)
        request.head.coroutine_uuid = uuid

        # 组包
        request_serial = package.serial_pb(request)
        branch = BranchSocket()
        branch_sock = branch.branch_sock.get(name)
        if branch_sock:
            timeout = branch_sock.get("timeout", None)
            g_log.debug("branch %s timeout %sms", name, timeout)
            timeout = float(timeout) / 1000

        result = 0
        if branch.send_to_branch(name, request_serial):
            result = coroutine_manage.coroutine_yield(uuid, timeout)
            if result == 0:
                pass
            elif result == "timeout":
                pass
            else:
                g_log.debug("return from branch %s", name)
        coroutine_manage.drop_coroutine_event(uuid)
        return result
    except Exception as e:
        g_log.error("%s", e)
        if uuid in dir():
            coroutine_manage.drop_coroutine_event(uuid)
        return 0


def receive_from_branch(name):
    """
    从旁路收包，检查包格式
    :param name: 旁路名称
    :return: 错误/0，(message, address)/成功
    """
    try:
        branch = BranchSocket()
        result = branch.receive_from_branch(name)
        if result == 0:
            return 0
        if package.check_message(result[0]) == 0:
            # 无效包，协程超时处理
            return 0
        return result
    except Exception as e:
        g_log.error("%s", e)
        return 0


def receive_from_sock(sock):
    """
    从socket收包，检查包格式
    :param sock: socket
    :return: 0/失败，(message, address)/成功
    """
    if sock:
        try:
            message, address = sock.recvfrom(65536)
            if package.check_message(message) == 0:
                # 无效包
                return 0
            return message, address
        except Exception as e:
            g_log.debug("%s", e)
            g_log.error("receive message from socket failed")
            return 0
    else:
        return 0


def send_to_sock(sock, message, address=None):
    if sock:
        try:
            if not address:
                sock.send(message)
            else:
                sock.sendto(message, address)
        except Exception as e:
            g_log.debug("%s", e)
            return 0
    else:
        return 0


def send_to_address(message, address):
    try:
        import socket
        sock = socket.socket(type=socket.SOCK_DGRAM)
        sock.connect(address)
        sock.send(message)
    except Exception as e:
        g_log.debug("%s", e)


if __name__ == "__main__":
    branch1 = BranchSocket()
    branch1.add_branch(name="branch1", address="127.0.0.1:9527", socket="socket1")
    branch2 = BranchSocket()
    branch2.add_branch(name="branch2", address="127.0.0.1:9528", socket="socket2")
    print(branch1, branch2)
    print branch1.branch_sock
    print branch2.branch_sock