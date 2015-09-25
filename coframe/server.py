# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import ConfigParser
import socket
import sys
import struct

import gevent
from gevent.core import loop
from gevent import Timeout

import calculus.wrapper.log as log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能，将这行代码放在import自定义模块最前面


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
                handler = {"name": section, "module": bmodule, "class": bcls, "timeout": bexpire}
            except ConfigParser.NoSectionError as e:
                g_log.warning("config file miss section %s" % section)
                handler = {}
            except ConfigParser.NoOptionError as e:
                g_log.warning("config file miss %s:%s", e.section, e.option)
                bmodule = Server.BMODULE if 'bmodule' not in dir() else bmodule
                bcls = Server.BCLASS if 'bcls' not in dir() else bcls
                bexpire = Server.BTIMEOUT if 'bexpire' not in dir() else bexpire
                handler = {"name": section, "module": bmodule, "class": bcls, "timeout": bexpire}
            except Exception as e:
                g_log.warning("%s", e)
                handler = {}
            if handler:
                g_log.debug("branch %s, handler %s:%s, timeout %sms", handler["name"], handler["module"],
                            handler["class"], handler["timeout"])
                self.branch_handler.append(handler)
        g_log.debug("analyze branch handler config done ...")
        # g_log.debug(self.branch_handler)
        g_log.debug("analyze branch config done ...")

        # 创建各旁路使用的socket
        for handler in self.branch_handler:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setblocking(0)
            index = self.branch_handler.index(handler)
            self.branch_handler[index]["sock"] = sock
            g_log.debug("create branch %s socket", handler["name"])
        g_log.debug("create branch socket done...")
        # g_log.debug(self.branch_handler)
        # pass

        # import master处理函数
        try:
            master_module = __import__(self.master[2], fromlist=[self.master[2]])
            # g_log.debug("%s", dir(master_module))
            self.master_class = getattr(master_module, self.master[3])
        except Exception as e:
            g_log.critical("%s", e)
            sys.exit(-1)
        g_log.debug("import master handler done...")

    def run(self):
        """
        注册master和branch协程
        启动loop，监听接入层请求，监听旁路回包
        接入层入口 self.master_watch
        旁路入口  self.branch_watch
        :return: 注册失败则程序推出
        """
        l = loop()

        # 注册master协程
        try:
            io = l.io(self.master_sock.fileno(), 1)   # 1代表read
            io.start(self.master_watch)
        except Exception as e:
            g_log.critical('error: %s', e)
            sys.exit(-1)
        g_log.debug("register master coroutine self.master_watch done ...")

        # 注册旁路协程
        for handler in self.branch_handler:
            try:
                sock = handler["sock"]
                io = l.io(sock.fileno(), 1)
                io.start(self.branch_watch)
            except Exception as e:
                g_log.critical('error:', e)
                sys.exit(-1)
            g_log.debug("register branch %s coroutine self.branch_watch done ...", handler["name"])
        g_log.debug("register branch coroutine done ...")

        # 启动libev loop
        l.run()
        # pass

    def master_watch(self):
        """
        处理收包，调用具体协议的业务逻辑
        调用master处理类中的enter()函数
        :return:
        """
        # 收包
        sock = self.master_sock
        data, addr = sock.recvfrom(65536)
        # g_log.debug("receive request, length %s", len(data))

        # 包有效性检查
        if len(data) < 8:   # 4Byte长度 ＋ {{ + PB + }}
            g_log("illegal package, not enough length" )
            return -1
        (packlen, stx) = struct.unpack('!I2s', data[0:6])
        if stx != "{{":
            g_log.warning("illegal package, illegal stx(%s)", stx)
            return -1

        if len(data) != (packlen + 4):
            g_log.warning("illegal package, illegal length")
            return -1

        etx = data[-2:]
        if etx != "}}":
            g_log.warning("illegal package, illegal etx(%s)", etx)
            return -1

        # 调用业务处理逻辑
        try:
            # master_module = __import__(self.master[2], fromlist=[self.master[2]])
            # g_log.debug("%s", dir(master_module))
            # master_class = getattr(master_module, self.master[3])
            master_obj = self.master_class(config=self.config)
            response = master_obj.enter(data)
            # g_log.debug(response)
            sock.sendto(response, addr)
        except Exception as e:
            # TODO 后台处理异常，暂时不回包
            g_log.critical("%s", e)
            sys.exit(-1)
            # g_log.debug("%s: end service logic ...", gevent.getcurrent())
            # pass
        # except Timeout as e:
        #     # 超时，关闭定时器，返回超时结果
        #     timeout.cancel()
        #     # print("i am here")
        #
        #     sock.sendto(, addr)


    def branch_watch(self):
        pass


if __name__ == "__main__":
    # print sys.path
    server = Server()
    # g_log.debug(dir(server))
    server.run()