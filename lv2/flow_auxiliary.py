# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from mongo_connection import get_mongo_collection


def upper_bound_update(**kwargs):
    """
    商家积分上限变更
    创建商家默认提供bound积分
    :param kwargs: {"numbers": 1000000, "merchant_identity": "", "bound": 1000}
    :return: (60100, "yes")/成功，(>60100, "errmsg")/失败
    """
    try:
        # 必须是已认证商家，在更新保证金已经做过验证，此处省略
        merchant_identity = kwargs.get("merchant_identity", "")

        bound = kwargs.get("bound", 0)
        value = {"merchant_identity": merchant_identity, "upper_bound": bound, "deleted": 0}

        # 存入数据库
        collection = get_mongo_collection("flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60113, "get collection flow failed"
        flow = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0}, {"$set": value})
        
        # 第一次更新，则插入一条
        if not flow:
            g_log.debug("insert new flow")
            flow = collection.insert_one(value)
        if not flow:
            g_log.error("update merchant %s credit upper bound failed", merchant_identity)
            return 60114, "update failed"
        g_log.debug("update upper bound succeed")
        return 60100, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 60117, "exception"