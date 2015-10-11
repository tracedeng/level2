# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


def singleton(cls, *args, **kwargs):
    """
    调用该方法实现单例，使用方法
    from singleton import singleton
    @singleton
    class ClassName():
    ⚠ 多个模块import单例类时路径要一致，否则当成不同的类
    :param cls:
    :param args:
    :param kw:
    :return:
    """
    instances = {}

    def _singleton():
        # print("instances = %s", instances)
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return _singleton