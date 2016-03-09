# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from mongo_connection import get_mongo_collection


def gift_upper_bound(**kwargs):
    """
    创建商家默认提供bound积分
    :param kwargs: {"numbers": 1000000, "merchant_identity": "", "bound": 1000}
    :return: (60100, "yes")/成功，(>60100, "errmsg")/失败
    """
    try:
        merchant_identity = kwargs.get("merchant_identity", "")

        bound = kwargs.get("bound", 0)
        value = {"merchant_identity": merchant_identity, "upper_bound": bound, "may_issued": 0, "issued": 0,
                 "interchange_in": 0, "interchange_out": 0, "consumption": 0, "balance": 0,
                 "deleted": 0}

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


def credit_exceed_upper(**kwargs):
    """
    商家是否允许发行本次积分
    allow_last是否允许透支一笔，互换时允许透支一笔，消费换积分不允许透支
    :param kwargs: {"merchant_identity": "", "credit": 1000, "allow_last": "yes"}
    :return: (60100, True|False)/成功，(>60100, "errmsg")/失败
    """
    try:
        merchant_identity = kwargs.get("merchant_identity", "")
        collection = get_mongo_collection("flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 61011, "get collection flow failed"
        flow = collection.find_one({"merchant_identity": merchant_identity, "deleted": 0})
        if not flow:
            g_log.error("merchant %s not exist", merchant_identity)
            return 61012, "merchant not exist"

        allow_last = kwargs.get("allow_last", "no")
        credit = kwargs.get("credit", 0)
        issued = flow["issued"]
        upper = flow["upper_bound"]

        if allow_last == "yes":
            if issued >= upper:
                g_log.debug("issued[%d] > upper[%d], credit[%d]", issued, upper, credit)
                return 61000, False
        else:
            if issued + credit >= upper:
                g_log.debug("issued[%d] + credit[%d] > upper[%d]", issued, credit, upper)
                return 61000, False

        return 61000, True
    except Exception as e:
        g_log.error("%s", e)
        return 61014, "exception"


def balance_overdraft(merchant_identity):
    """
    商家账户是否有余额
    :param merchant_identity: "merchant_identity"
    :return: (61100, yes)/有，(>61100, "errmsg")/无
    """
    try:
        collection = get_mongo_collection("flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 61111, "get collection flow failed"
        flow = collection.find_one({"merchant_identity": merchant_identity, "deleted": 0})
        if not flow:
            g_log.error("merchant %s not exist", merchant_identity)
            return 61112, "merchant not exist"

        balance = int(flow["balance"])
        if balance <= 0:
            g_log.debug("balance[%d] <= 0, overdraft", balance)
            return 61113, "balance overdraft"

        return 61100, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 61114, "exception"
