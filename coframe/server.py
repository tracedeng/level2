# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import ConfigParser
import socket
import sys

from gevent.core import loop

import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能，将这行代码放在import自定义模块最前面
import branch_socket


class Server():
    # IP = "0.0.0.0"
    IP = "127.0.0.1"
    PORT = 9526
    MMODULE = "calculus.lv2.master"
    MCLASS = "Master"
    MTIMEOUT = 2000    # 单位毫秒
    BMODULE = "calculus.lv2.branchbase"
    BCLASS = "BranchBase"
    BTIMEOUT = 500    # 单位毫秒
    BADDRESS = "127.0.0.1:9527"

    def __init__(self, conf_path="loop.conf"):
        """
        解析配置，创建监听和旁路套结字
        配置文件格式
        [master]
        ip : localhost
        port : 9527
        module : test.lv2.master    ; import模块
        class : Master  ; 处理类

        ; 旁路
        [branch]
        ;count : 0  ; 旁路总量，旁路详情参考具体
        list : vip,branch2,branch3  ;旁路列表

        ; 旁路，用户会员VIP策略
        [vip]
        module : test.lv2.branchbase  ; 旁路对应的处理模块
        class : BranchBase  ; 继承BranchBase

        ; 旁路
        [branch2]
        module : test.lv2.branchbase  ; 旁路对应的处理模块
        class : BranchBase  ; 继承BranchBase
        :param conf_path: 配置文件路径
        :return:
        """
        config = ConfigParser.ConfigParser()
        config.read(conf_path)
        self.config = config

        # 解析监听IP和端口，master处理模块和类
        try:
            ip = config.get("master", "ip")
            port = config.get("master", "port")
            module = config.get("master", "module")
            cls = config.get("master", "class")
            expire = config.get("master", "timeout")
            self.master = (ip, int(port), module, cls, expire)
        except ConfigParser.NoSectionError as e:
            g_log.warning("config file miss section master, listen on 127.0.0.1:9526, "
                          "handler test.level2.master:Master, timeout 2 seconds")
            self.master = (Server.IP, Server.PORT, Server.MMODULE, Server.MCLASS, Server.MTIMEOUT)
        except ConfigParser.NoOptionError as e:
            g_log.warning("config file miss %s:%s", e.section, e.option)
            ip = Server.IP if 'ip' not in dir() else ip
            port = Server.PORT if 'port' not in dir() else port
            module = Server.MMODULE if 'module' not in dir() else module
            cls = Server.MCLASS if 'cls' not in dir() else cls
            expire = Server.MTIMEOUT if 'expire' not in dir() else expire
            self.master = (ip, int(port), module, cls, expire)
        except Exception as e:
            g_log.warning("%s", e)
            self.master = (Server.IP, Server.PORT, Server.MMODULE, Server.MCLASS, Server.MTIMEOUT)

        g_log.debug("listen on %s:%s, handler %s:%s, timeout %sms" % self.master)
        g_log.debug("%s", self.master)
        g_log.debug("analyze master config done ...")

        # 创建接入使用的socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self.master[0:2])
        sock.setblocking(0)
        self.master_sock = sock  # 接收接入层请求socket
        g_log.debug("create master socket done ...")

        # 解析旁路列表
        try:
            branch = config.get("branch", "list")
            branch = branch.split(",")
            g_log.debug("branch list: %s", branch)
        except ConfigParser.NoSectionError as e:
            g_log.warning("config file miss section branch, so no bypass")
        except ConfigParser.NoOptionError as e:
            g_log.warning("config file miss branch:count option, so no bypass")
        except Exception as e:
            g_log.warning("%s", e)
        self.branch = [] if "branch" not in dir() else branch
        g_log.debug("analyze branch list config done ...")

        # 解析各旁路处理模块和类
        self.branch_handler = []
        for section in self.branch:
            try:
                bmodule = config.get(section, "module")
                bcls = config.get(section, "class")
                bexpire = config.get(section, "timeout")
                baddress = config.get(section, "address")
                handler = {"name": section, "module": bmodule, "class": bcls, "timeout": bexpire, "address": baddress}
            except ConfigParser.NoSectionError as e:
                g_log.warning("config file miss section %s" % section)
                handler = {}
            except ConfigParser.NoOptionError as e:
                g_log.warning("config file miss %s:%s", e.section, e.option)
                bmodule = Server.BMODULE if 'bmodule' not in dir() else bmodule
                bcls = Server.BCLASS if 'bcls' not in dir() else bcls
                bexpire = Server.BTIMEOUT if 'bexpire' not in dir() else bexpire
                baddress = Server.BADDRESS if 'baddress' not in dir() else baddress
                handler = {"name": section, "module": bmodule, "class": bcls, "timeout": bexpire, "address": baddress}
            except Exception as e:
                g_log.warning("%s", e)
                handler = {}
            if handler:
                g_log.debug("branch %s, handler %s:%s, timeout %sms, address %s", handler["name"], handler["module"],
                            handler["class"], handler["timeout"], handler["address"])
                self.branch_handler.append(handler)
        g_log.debug("analyze branch handler config done ...")
        # g_log.debug(self.branch_handler)
        g_log.debug("analyze branch config done ...")

        # 创建各旁路使用的socket
        for handler in self.branch_handler:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setblocking(0)
            # index = self.branch_handler.index(handler)
            # self.branch_handler[index]["sock"] = sock
            handler["sock"] = sock
            g_log.debug("create branch %s socket", handler["name"])
            # 旁路socket通讯初始化
            comm = branch_socket.BranchSocket()
            if 0 == comm.add_branch(name=handler["name"], socket=sock, address=handler["address"],
                                    timeout=handler["timeout"]):
                # g_log.critical("")
                sys.exit(-1)
        g_log.debug("create branch socket done...")

        # import master处理函数
        try:
            master_module = __import__(self.master[2], fromlist=[self.master[2]])
            # g_log.debug("%s", dir(master_module))
            self.master_class = getattr(master_module, self.master[3])
        except Exception as e:
            g_log.critical("%s", e)
            sys.exit(-1)
        g_log.debug("import master handler done...")

        # import branch处理函数
        try:
            for handler in self.branch_handler:
                branch_module = __import__(handler["module"], fromlist=handler["module"])
                handler["class_real"] = getattr(branch_module, handler["class"])
                g_log.debug("import branch %s handle", handler["name"])
        except Exception as e:
            g_log.critical("%s", e)
            sys.exit(-1)
        g_log.debug("import branch handler done...")

    def run(self):
        """
        注册master和branch协程
        启动loop，监听接入层请求，监听旁路回包
        接入层入口 self.master_watch
        旁路入口  self.branch_watch
        :return: 注册失败则程序推出
        """
        l = loop()

        # 注册master处理方法
        try:
            io = l.io(self.master_sock.fileno(), 1)   # 1代表read
            io.start(self.master_watch)
        except Exception as e:
            g_log.critical('error: %s', e)
            sys.exit(-1)
        g_log.debug("register master coroutine self.master_watch done ...")

        # 注册旁路处理方法
        for handler in self.branch_handler:
            try:
                sock = handler["sock"]
                io = l.io(sock.fileno(), 1)
                io.start(self.branch_watch(handler["name"]))
            except Exception as e:
                g_log.critical('error:', e)
                sys.exit(-1)
            g_log.debug("register branch %s coroutine self.branch_watch done ...", handler["name"])
        g_log.debug("register branch coroutine done ...")

        # 启动libev loop
        g_log.debug("now start event loop...")
        l.run()

    def master_watch(self):
        """
        处理收包，调用具体协议的业务逻辑
        调用master处理类中的enter()函数
        :return:
        """
        try:
            # 收包，放在这的原因是，如果包错误（可能性很大），省掉类实例话操作
            result = branch_socket.receive_from_sock(self.master_sock)
            if result == 0:
                return 0
            message, address = result

            # 调用业务处理逻辑
            master_obj = self.master_class(config=self.config)
            response = master_obj.enter(message)
            if response == 0:
                # 非法包，不处理，不回包
                return 0
            # g_log.debug(response)
            self.master_sock.sendto(response, address)
        except Exception as e:
            # TODO 后台处理异常，暂时不回包
            g_log.critical("%s", e)
            return 0

    def branch_watch(self, name):
        for handler in self.branch_handler:
            if handler["name"] == name:
                break

        def branch_watch_really():
            try:
                # 收包
                result = branch_socket.receive_from_branch(name)
                if result == 0:
                    return 0
                message, address = result

                # 调用旁业务处理逻辑
                branch_obj = handler["class_real"](name)
                branch_obj.enter(message)
                return 0
            except Exception as e:
                # TODO 后台处理异常，协程超时处理
                g_log.critical("%s", e)
                return 0
                # sys.exit(-1)
        return branch_watch_really


if __name__ == "__main__":
    # print sys.path
    server = Server()
    # g_log.debug(dir(server))
    server.run()