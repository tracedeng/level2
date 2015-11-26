# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

from datetime import datetime
from pymongo.collection import ReturnDocument
from mongo_connection import get_mongo_collection
import common_pb2
import package
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from account_valid import account_is_valid_merchant, account_is_platform
from merchant import user_is_merchant_manager, merchant_is_verified, merchant_retrieve_with_merchant_identity_only, \
    merchant_material_copy_from_document


class Flow():
    """
    商家积分变动模块，命令号<600
    request：请求包解析后的pb格式
    """
    def __init__(self, request):
        self.request = request
        self.head = request.head
        self.cmd = self.head.cmd
        self.seq = self.head.seq
        self.numbers = self.head.numbers
        self.code = 1   # 模块号(2位) + 功能号(2位) + 错误号(2位)
        self.message = ""

    def enter(self):
        """
        处理具体业务
        :return: 0/不回包给前端，pb/正确返回，timeout/超时
        """
        # TODO... 验证登录态
        try:
            command_handle = {501: self.merchant_credit_flow_retrieve, 502: self.dummy_command}

            result = command_handle.get(self.cmd, self.dummy_command)()
            if result == 0:
                # 错误或者异常，不回包
                response = 0
            elif result == 1:
                # 错误，且回包
                response = package.error_response(self.cmd, self.seq, self.code, self.message)
            else:
                # 正确，回包
                response = result
            return response
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def merchant_credit_flow_retrieve(self):
        """
        读取商家积分详情
        :return:
        """
        try:
            body = self.request.merchant_credit_flow_retrieve_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity

            if not numbers:
                # TODO... 根据包体中的identity获取numbers
                pass

            if merchant_identity:
                g_log.debug("%s retrieve merchant %s credit", numbers, merchant_identity)
            else:
                g_log.debug("%s retrieve all merchant credit", numbers)
            self.code, self.message = merchant_credit_flow_retrieve(numbers, merchant_identity)

            if 60100 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "retrieve merchant credit done"

                flow_record = response.merchant_credit_flow_retrieve_response.flow_record
                last_merchant = ""
                # 遍历管理员所有商家
                for value in self.message:
                    if last_merchant != value["merchant_identity"]:
                        flow_record_one = flow_record.add()
                        # 商家资料
                        code, merchants = merchant_retrieve_with_merchant_identity_only(value["merchant_identity"])
                        merchant_material_copy_from_document(flow_record_one.merchant, merchants[0])
                        material = flow_record_one.material
                    # aggressive_record_one = aggressive_record.add()
                    last_merchant = value["merchant_identity"]
                    merchant_flow_copy_from_document(material, value)

                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def merchant_settlement(self):
        pass

    def dummy_command(self):
        # 无效的命令，不回包
        g_log.debug("unknow command %s", self.cmd)
        return 0


def enter(request):
    """
    模块入口
    :param request: 解析后的pb格式
    :return: 0/不回包给前端，pb/正确返回，timeout/超时
    """
    try:
        flow = Flow(request)
        return flow.enter()
    except Exception as e:
        g_log.error("%s", e)
        return 0


def bond_to_upper_bound(bond):
    """
    根据保证金计算商家积分上限
    :param bond:
    :return:
    """
    return bond


def upper_bound_update(**kwargs):
    """
    商家积分上限变更，保证金变更的时候调用
    :param kwargs: {"numbers": 1000000, "merchant_identity": "", "manager": 11868898224, "bond": 1000}
    :return: (50100, "yes")/成功，(>50100, "errmsg")/失败
    """
    try:
        # 检查请求用户numbers必须是平台管理员
        numbers = kwargs.get("numbers", "")
        if not account_is_platform(numbers):
            g_log.warning("not platform %s", numbers)
            return 60101, "no privilege"
        # 必须是已认证商家，在更新保证金已经做过验证，此处省略

        merchant_identity = kwargs.get("merchant_identity", "")
        # TODO... 保证金检查
        bond = kwargs.get("bond", 0)
        upper_bound = bond_to_upper_bound(bond)
        value = {"merchant_identity": merchant_identity, "upper_bound": upper_bound, "deleted": 0}

        # 存入数据库
        collection = get_mongo_collection(numbers, "flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60103, "get collection flow failed"
        flow = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0}, {"$set": value})

        # 第一次更新，则插入一条
        if not flow:
            g_log.debug("insert new flow")
            # value["balance"] = 0
            # value["consumption_ratio"] = 0
            flow = collection.insert_one(value)
        if not flow:
            g_log.error("update merchant %s credit upper bound failed", merchant_identity)
            return 60104, "update failed"
        g_log.debug("update upper bound succeed")
    except Exception as e:
        g_log.error("%s", e)
        return 50107, "exception"


# def total_update(**kwargs):
#     """
#     可发行积分总量变更
#     未认证商家，可发行积分总量固定值，
#     已认证商家可通过充值转换成可发行积分总量
#     :param kwargs: {"numbers": 11868898224, "merchant_identity": "", "supplement": 1000}
#     :return: (50100, "yes")/成功，(>50100, "errmsg")/失败:
#     """
#     try:
#         # 检查请求用户numbers必须是平台管理员
#         numbers = kwargs.get("numbers", "")
#         if not account_is_valid_merchant(numbers):
#             g_log.warning("not manager %s", numbers)
#             return 60101, "no privilege"
#         # 必须是已认证商家，在补充可发行积分总量时已经做过验证，此处省略
#
#         merchant_identity = kwargs.get("merchant_identity", "")
#         merchant = user_is_merchant_manager(numbers, merchant_identity)
#         if not merchant:
#             g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
#             return 50402, "not manager"
#         merchant_founder = merchant["merchant_founder"]
#         g_log.debug("merchant %s founder %s", merchant_identity, merchant_founder)
#
#         # TODO... 保证金检查
#         supplement = kwargs.get("supplement", 0)
#         value = {"merchant_identity": merchant_identity, "supplement": supplement, "deleted": 0}
#
#         # 存入数据库
#         collection = get_mongo_collection(numbers, "flow")
#         if not collection:
#             g_log.error("get collection flow failed")
#             return 60103, "get collection flow failed"
#         flow = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
#                                               {"$inc": {"total": supplement}})
#
#         # 第一次更新，则插入一条
#         if not flow:
#             g_log.debug("insert new flow")
#             # value["balance"] = 0
#             # value["consumption_ratio"] = 0
#             flow = collection.insert_one(value)
#         if not flow:
#             g_log.error("update merchant %s credit total failed", merchant_identity)
#             return 60104, "update failed"
#         g_log.debug("update total credit succeed")
#     except Exception as e:
#         g_log.error("%s", e)
#         return 50107, "exception"


def merchant_credit_update(**kwargs):
    """
    商家积分变更
    积分类型：可发行积分总量、已发行积分、积分互换IN & OUT、用户消费互换的积分变更
    mode=["may_issued", "issued", "interchange_in", "interchange_out", "interchange_consumption"]
    :param kwargs: {"numbers": 11868898224, "merchant_identity": "", "mode": may_issued, "supplement": 1000}
    :return:
    """
    try:
        # 检查请求用户numbers必须是平台管理员
        numbers = kwargs.get("numbers", "")
        if not account_is_valid_merchant(numbers):
            g_log.warning("not manager %s", numbers)
            return 60101, "no privilege"
        # 必须是已认证商家，在补充可发行积分总量时已经做过验证，此处省略

        merchant_identity = kwargs.get("merchant_identity", "")
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 60402, "not manager"
        merchant_founder = merchant["merchant_founder"]
        g_log.debug("merchant %s founder %s", merchant_identity, merchant_founder)

        mode = kwargs.get("mode", "")
        modes = ["may_issued", "issued", "interchange_in", "interchange_out", "interchange_consumption"]
        if mode not in modes:
            g_log.error("not supported mode %s", mode)
            return 60403, "not supported mode"
        # TODO... 积分检查
        supplement = kwargs.get("supplement", 0)
        value = {"merchant_identity": merchant_identity, mode: supplement, "deleted": 0}

        # 存入数据库
        collection = get_mongo_collection(numbers, "flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60403, "get collection flow failed"
        flow = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
                                              {"$inc": {mode: supplement}})

        # 第一次更新，则插入一条
        if not flow:
            g_log.debug("insert new flow")
            flow = collection.insert_one(value)
        if not flow:
            g_log.error("update merchant %s %s credit failed", merchant_identity, mode)
            return 60404, "update failed"
        g_log.debug("update merchant %s credit succeed", mode)
    except Exception as e:
        g_log.error("%s", e)
        return 60407, "exception"


def merchant_credit_flow_retrieve(numbers, merchant_identity):
    """
    读取商家积分详情，没给出merchant_identity则读取全部
    :param numbers: 平台账号或管理员账号
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        if not merchant_identity:
            # 平台读取所有操作纪录
            return merchant_credit_flow_retrieve_all(numbers)

        # 检查管理员和商家关系
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 60102, "not manager"

        collection = get_mongo_collection(numbers, "flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60103, "get collection flow failed"
        records = collection.find({"merchant_identity": merchant_identity, "deleted": 0})
        if not records:
            g_log.error("retrieve flow failed")
            return 60104, "retrieve failed"

        return 60100, records
    except Exception as e:
        g_log.error("%s", e)
        return 60105, "exception"


def merchant_credit_flow_retrieve_all(numbers):
    """
    查找所有商家的操作纪录
    :param numbers:
    :return:
    """
    try:
        if not account_is_platform(numbers):
            g_log.error("%s not platform", numbers)
            return 60106, "no privilege"

        # 广播查找所有商家的积分详情
        collection = get_mongo_collection(numbers, "flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60107, "get collection flow failed"
        records = collection.find({"deleted": 0}).sort("merchant_identity")
        if not records:
            g_log.error("retrieve flow failed")

        return 60100, records
    except Exception as e:
        g_log.error("%s", e)
        return 60108, "exception"


def calculate_settlement(numbers, merchant_identity):
    """
    计算商家可结算的积分
    :param numbers: 平台账号或管理员账号
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        # TODO... 支持批量查询
        # if not merchant_identity:
        #     return merchant_credit_flow_retrieve_all(numbers)

        # 检查管理员和商家关系
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 60102, "not manager"

        collection = get_mongo_collection(numbers, "flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60103, "get collection flow failed"
        record = collection.find_one({"merchant_identity": merchant_identity, "deleted": 0})
        if not record:
            g_log.error("retrieve flow failed")
            return 60104, "retrieve failed"
        settlement = record["interchange_in"] - record["interchange_out"] - record["settlement"]

        return 60100, settlement
    except Exception as e:
        g_log.error("%s", e)
        return 60105, "exception"


def exec_settlement(numbers, merchant_identity):
    """
    商家积分结算
    :param numbers: 平台账号或管理员账号
    :param merchant_identity: 商家ID
    """
    try:
        # TODO... 支持批量结算
        # if not merchant_identity:
        #     return merchant_credit_flow_retrieve_all(numbers)

        # 检查管理员和商家关系
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 60102, "not manager"

        collection = get_mongo_collection(numbers, "flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60103, "get collection flow failed"
        # TODO... How to update flow set settlement = (interchange_in - interchange_out - settlement)
        # TODO...         where merchant_identity = "" and "deleted" = 0
        # record = collection.find_one({"merchant_identity": merchant_identity, "deleted": 0})
        # if not record:
        #     g_log.error("retrieve flow failed")
        #     return 60104, "retrieve failed"
        # settlement = record["interchange_in"] - record["interchange_out"] - record["settlement"]

        return 60100, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 60105, "exception"


def merchant_flow_copy_from_document(material, value):
    material.upper_bound = int(value["upper_bound"])
    material.may_issued = int(value["may_issued"])
    material.issued = int(value["issued"])
    material.interchange_in = int(value["interchange_in"])
    material.interchange_out = int(value["interchange_out"])
    material.interchange_consumption = int(value["interchange_consumption"])
    material.identity = str(value["_id"])


# 测试时mongo_connection的配置文件路径写全
if "__main__" == __name__:
    kwargs1 = {"numbers": "118688982240", "merchant_identity": "562c7ad6494ac55faf750798", "bond": 100}
    upper_bound_update(**kwargs1)
    kwargs1 = {"numbers": "118688982240", "merchant_identity": "562c7ad6494ac55faf750798", "supplement": 1002}
    for mode1 in ["may_issued", "issued", "interchange_in", "interchange_out", "interchange_consumption"]:
        kwargs1["mode"] = mode1
        merchant_credit_update(**kwargs1)
