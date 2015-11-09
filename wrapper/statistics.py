# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


from datetime import datetime
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from singleton import singleton


@singleton
class Statistics():
    """
    每分钟上报到统计服务器一次，每个进程上报各自独立
    定义统计数据结构 => [{"attribute": 9527, "minute": "201511111111", "quantity": 9527}, ...]
    number：编号
    quantity：每分钟统计量
    minute：精确到分钟
    """
    def __init__(self):
        self.attribute_index = {}
        self.attribute_minute_quantity = []
        self.sock = None

    def report(self, attribute, quantity):
        """
        上报
        :param attribute: 上报属性
        :param quantity: 上报量
        :return:
        """
        try:
            now_minute = datetime.now().strftime("%Y%m%d%H%M")
            index = self.attribute_index.get(attribute, None)
            if None != index:
                item = self.attribute_minute_quantity[index]
                if now_minute == item["minute"]:
                    item["quantity"] += quantity
                else:
                    # 超过1分钟统计量
                    self.deliver_2_server(index)
                    item["attribute"] = attribute
                    item["minute"] = now_minute
                    item["quantity"] = quantity
            else:
                # 属性第一次上报
                count = len(self.attribute_minute_quantity)
                self.attribute_minute_quantity.append({"attribute": attribute, "minute": now_minute,
                                                       "quantity": quantity})
                self.attribute_index[attribute] = count
                g_log.debug("now attribute count %s", count + 1)
                if count >= 999:
                    # 超过1000总统计量
                    g_log.warning("statistics attribute exceed 1000")
        except Exception as e:
            g_log.error("<%s> %s", e, e.__class__)

    def report_one(self, attribute):
        """
        上报，量为1
        :param attribute: 上报属性
        :return:
        """
        try:
            self.report(attribute, 1)
        except Exception as e:
            g_log.error("<%s> %s", e, e.__class__)

    def dump(self):
        try:
            g_log.debug(self.attribute_index)
            g_log.debug(self.attribute_minute_quantity)
        except Exception as e:
            g_log.error("<%s> %s", e, e.__class__)

    def deliver_2_server(self, index):
        # TODO...发送给统计服务器
        pass


# 语法糖
def report(attribute, quantity):
    """
    上报
    :param attribute: 上报属性
    :param quantity: 上报量
    :return:
    """
    try:
        statistics = Statistics()
        statistics.report(attribute, quantity)
    except Exception as e:
            g_log.error("<%s> %s", e, e.__class__)


def report_one(attribute):
    """
    上报，量为1
    :param attribute: 上报属性
    :return:
    """
    try:
        statistics = Statistics()
        statistics.report(attribute, 1)
    except Exception as e:
        g_log.error("<%s> %s", e, e.__class__)


if "__main__" == __name__:
    import time
    start = time.clock()
    g_log.debug(start)
    for i in xrange(0, 10000):
        report(1000, 10)
    for i in xrange(0, 100000):
        report(100, 20)
    for i in xrange(0, 10000):
        report(200, 5)
    end = time.clock()
    g_log.debug(end - start)
    g_log.debug(end)

    statistics1 = Statistics()
    statistics1.dump()
    # statistics2 = Statistics()
    # statistics2.dump()