# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import ConfigParser

import gevent
from gevent import Timeout

import common_pb2
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
import package

import account
import consumer
import merchant
import credit
import business
import flow
import qiniu_token
import activity
import voucher
import maintain

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
        # g_log.debug("master timeout %sms", expire)
        self.expire = float(expire) / 1000  # 转换成秒

    def enter(self, message):
        """
        主线处理入口，启动定时器，解析请求，返回业务逻辑处理结果
        :param message: 请求包
        :return: 0/不回包，pb/超时、程序异常、正常返回
        """
        timeout = gevent.Timeout(self.expire)
        timeout.start()
        try:
            g_log.debug("%s: begin service logic ...", gevent.getcurrent())
            request = common_pb2.Request()
            request.ParseFromString(message[6:-2])
            g_log.debug("receive request %s", request)
            head = request.head

            if head.cmd < 100:
                # 注册登录模块
                response = account.enter(request)
            elif head.cmd < 200:
                # 用户资料模块
                response = consumer.enter(request)
            elif head.cmd < 300:
                # 商户资料模块
                response = merchant.enter(request)
            elif head.cmd < 400:
                # 积分模块
                response = credit.enter(request)
            elif head.cmd < 500:
                # 商家经营参数
                response = business.enter(request)
            elif head.cmd < 600:
                # 商家积分流动模块
                response = flow.enter(request)
            elif head.cmd < 700:
                # 七牛云存储
                response = qiniu_token.enter(request)
            elif head.cmd < 800:
                # 活动
                response = activity.enter(request)
            elif head.cmd < 900:
                response = voucher.enter(request)
            elif head.cmd < 1000:
                response = maintain.enter(request)
            else:
                # 非法请求，无效命令，不回包
                # return 0
                response = 0
            if response == "timeout":
                response = package.timeout_response(head.cmd, head.seq)
        except Timeout as e:
            # 超时，关闭定时器
            # timeout.cancel()
            g_log.debug("%s", e)
            g_log.debug("deal request timeout")
            response = package.timeout_response(head.cmd, head.seq)
            # message = package.serial_pb(response)
        except Exception as e:
            g_log.error("%s", e)
            response = package.exception_response(head.cmd, head.seq)
            # message = package.serial_pb(response)
        g_log.debug("response %s", response)

        try:
            if response == 0:
                return 0
            else:
                message = package.serial_pb(response)
                return message
        finally:
            g_log.debug("%s: end service logic ...", gevent.getcurrent())
            timeout.cancel()
            # return message
