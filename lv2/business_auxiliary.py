# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from mongo_connection import get_mongo_collection
from account import account_is_valid_merchant
from datetime import datetime


# pragma 更新消费换积分比率API
def consumption_ratio_update_when_register(**kwargs):
    """
    更新消费换积分比率，未认证商家也可以操作
    :param kwargs: {"numbers": 118688982240, "merchant_identity": "", "consumption_ratio": 100}
    :return: (50400, "yes")/成功，(>50400, "errmsg")/失败
    """
    try:
        # 检查要请求用户numbers必须是平台管理员
        numbers = kwargs.get("numbers", "")
        if not account_is_valid_merchant(numbers):
            g_log.warning("not manager %s", numbers)
            return 50421, "not manager"

        # 检查管理员和商家关系
        merchant_identity = kwargs.get("merchant_identity", "")
        
        # TODO... 消费换积分比率检查
        consumption_ratio = kwargs.get("consumption_ratio", 0)
        value = {"merchant_identity": merchant_identity, "consumption_ratio": consumption_ratio, "deleted": 0}

        # 存入数据库
        collection = get_mongo_collection("parameters")
        if not collection:
            g_log.error("get collection parameters failed")
            return 50423, "get collection parameters failed"
        business_parameters = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
                                                             {"$set": value})
        # 第一次更新，则插入一条
        if not business_parameters:
            g_log.debug("insert new parameters")
            value["bond"] = 0
            value["balance"] = 0
            value["balance_ratio"] = 0
            business_parameters = collection.insert_one(value)
        if not business_parameters:
            g_log.error("update merchant %s parameters failed", merchant_identity)
            return 50424, "update failed"

        # business_parameters = collection.find_one_and_replace({"merchant_identity": merchant_identity, "deleted": 0},
        #                                                       value, upsert=True, return_document=ReturnDocument.AFTER)
        # if not business_parameters:
        #     g_log.error("update merchant %s parameters failed", merchant_identity)
        #     return 50424, "update failed"
        # g_log.debug("update consumption done")

        # 更新记录入库
        collection = get_mongo_collection("parameters_record")
        if not collection:
            g_log.error("get collection parameters record failed")
            return 50425, "get collection parameters record failed"
        quantization = "consumption_ratio:%s" % (consumption_ratio,)
        result = collection.insert_one({"merchant_identity": merchant_identity, "time": datetime.now(),
                                        "operator": numbers, "quantization": quantization})
        if not result:
            g_log.error("insert parameters record failed")
            # return 50426, "insert parameters record failed"
        return 50400, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 50427, "exception"