# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import redis
import ConfigParser
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from singleton import singleton


@singleton
class RedisConnection():
    CONFPATH = "loop.conf"   # 配置文件路径

    def __init__(self):
        """
        解析配置，创建redis连接，所有连接存在self.redis_connection列表中
        配置文件格式
        [redis]
        0 : 127.0.0.1:6379:0    ;redis地址  ip:port:db
        1 : 127.0.0.1:6379:1
        :param:
        :return:
        """

        config = ConfigParser.ConfigParser()
        config.read(self.__class__.CONFPATH)
        # self.config = config

        # 解析redis数据库数量
        self.count = 0
        try:
            self.count = config.getint("redis", "count")
        except ConfigParser.NoSectionError as e:
            g_log.warning("config file miss section redis, consider redis server count 1")
        except ConfigParser.NoOptionError as e:
            g_log.warning("config file miss redis:count, consider redis server count 1")
        except Exception as e:
            g_log.warning("%s", e)
        g_log.debug("redis count %s", self.count)

        # 创建redis数据库连接
        self.redis_connection = []
        for index in xrange(0, self.count):
            try:
                address = config.get("redis", str(index))
                ip, port, db, password = address.split(":")
                connection = redis.Redis(host=ip, port=port, db=db, password=password)
                g_log.debug("%s:create redis connection to %s:%s:%s", index, ip, port, db)
            except ConfigParser.NoSectionError as e:
                g_log.warning("config file miss section redis, consider connection None")
                connection = None
            except ConfigParser.NoOptionError as e:
                g_log.warning("config file miss %s:%s", e.section, e.option)
                connection = None
            except Exception as e:
                g_log.warning("%s", e)
                connection = None
            if None == connection:
                g_log.warning("%s:create redis connection None", index)
            self.redis_connection.append(connection)
        g_log.debug("create redis connection done ...")
        g_log.debug("analyze redis config done ...")

    def get_redis_connection(self, index):
        """
        返回第index的redis连接
        :param index: redis连接序号
        :return: 0/失败, redis连接/成功
        """
        try:
            connection = self.redis_connection[index]
        except Exception as e:
            g_log.warning("%s", e)
            connection = 0
        return connection

    @staticmethod
    def route_to_index(route):
        """
        路由算法
        :param route:
        """
        # TODO... 路由算法
        g_log.debug("%s route to 0", route)
        return 0


# 语法糖
def get_redis_connection(route):
    """
    找到路由到的redis，并返回连接
    :param route: 路由key
    :return: 0/失败, redis连接/成功
    """
    try:
        redis_connection = RedisConnection()
        return redis_connection.get_redis_connection(redis_connection.__class__.route_to_index(route))
    except Exception as e:
        g_log.debug("%s", e)
        return 0

if "__main__" == __name__:
    redis_connection1 = RedisConnection()
    for i in xrange(0, redis_connection1.count):
        g_log.debug("%s: %s", i, redis_connection1.get_redis_connection(i))
    redis_connection2 = RedisConnection()
    for i in xrange(0, redis_connection2.count):
        g_log.debug("%s: %s", i, redis_connection2.get_redis_connection(i))