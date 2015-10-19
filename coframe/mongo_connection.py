# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import pymongo
import urllib
import ConfigParser
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from singleton import singleton


@singleton
class MongoConnection():
    CONFPATH = "loop.conf"   # 配置文件路径

    def __init__(self):
        """
        解析配置，创建mongo连接，所有连接存在self.mongo_connection列表中
        配置文件格式
        [mongo]
        0 : 127.0.0.1:6379:0    ;mongo地址  ip:port:db
        1 : 127.0.0.1:6379:1
        :param:
        :return:
        """

        config = ConfigParser.ConfigParser()
        config.read(self.__class__.CONFPATH)
        # self.config = config

        # 解析mongo数据库数量
        self.count = 0
        try:
            self.count = config.getint("mongo", "count")
        except ConfigParser.NoSectionError as e:
            g_log.warning("config file miss section mongo, consider mongo server count 1")
        except ConfigParser.NoOptionError as e:
            g_log.warning("config file miss mongo:count, consider mongo server count 1")
        except Exception as e:
            g_log.warning("%s", e)
        g_log.debug("mongo count %s", self.count)

        # 创建mongo数据库连接
        self.mongo_connection = []
        for index in xrange(0, self.count):
            try:
                address = config.get("mongo", str(index))
                ip, port, db, user, password = address.split(":")
                if user and password:
                    password = urllib.quote_plus(password)
                    uri = 'mongodb://%s:%s@%s/%s?authMechanism=SCRAM-SHA-1' % (user, password, ip, db)
                else:
                    uri = 'mongodb://%s/%s' % (ip, db)
                # g_log.debug(uri)
                connection = pymongo.MongoClient(uri, port=int(port))
                g_log.debug("%s:create mongo connection to %s:%s:%s", index, ip, port, db)
            except ConfigParser.NoSectionError as e:
                g_log.warning("config file miss section mongo, consider connection None")
                connection = None
            except ConfigParser.NoOptionError as e:
                g_log.warning("config file miss %s:%s", e.section, e.option)
                connection = None
            except Exception as e:
                g_log.warning("%s", e)
                connection = None
            if None == connection:
                g_log.warning("%s:create mongo connection None", index)
            self.mongo_connection.append(connection)
        g_log.debug("create mongo connection done ...")
        g_log.debug("analyze mongo config done ...")

    def get_mongo_connection(self, index):
        """
        返回第index的mongo连接
        :param index: mongo连接序号
        :return: 0/失败, mongo连接/成功
        """
        try:
            connection = self.mongo_connection[index]
        except Exception as e:
            g_log.warning("%s", e)
            connection = 0
        return connection

    def get_mongo_collection(self, index, collection):
        """
        返回第index的mongo连接的collection
        :param index: mongo连接序号
        :param collection: 连接对应数据库的collection
        :return: 0/失败, mongo collection/成功
        """
        try:
            connection = self.get_mongo_connection(index)
            return connection.get_default_database()[collection]
        except Exception as e:
            g_log.warning("%s", e)
            return 0

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
def get_mongo_connection(route):
    """
    找到路由到的mongo，并返回连接
    :param route: 路由key
    :return: 0/失败, mongo连接/成功
    """
    try:
        mongo_connection = MongoConnection()
        return mongo_connection.get_mongo_connection(mongo_connection.__class__.route_to_index(route))
    except Exception as e:
        g_log.debug("%s", e)
        return 0


def get_mongo_collection(route, collection):
    """
    返回第index的mongo连接对应数据库的collection
    :param route: 路由key
    :param collection: 连接对应数据库的collection
    :return: 0/失败, mongo collection/成功
    """
    try:
        mongo_connection = MongoConnection()
        return mongo_connection.get_mongo_collection(mongo_connection.__class__.route_to_index(route), collection)
    except Exception as e:
        g_log.debug("%s", e)
        return 0

if "__main__" == __name__:
    mongo_connection1 = MongoConnection()
    for i in xrange(0, mongo_connection1.count):
        g_log.debug("%s: %s", i, mongo_connection1.get_mongo_connection(i))
    mongo_connection2 = MongoConnection()
    for i in xrange(0, mongo_connection2.count):
        g_log.debug("%s: %s", i, mongo_connection2.get_mongo_connection(i))

    collection = mongo_connection1.get_mongo_collection(0, "merchant")
    g_log.debug(collection.__class__)
    # g_log.debug("asdf")