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
from account_auxiliary import verify_session_key, identity_to_numbers
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
        self.session_key = self.head.session_key

        self.code = 1   # 模块号(2位) + 功能号(2位) + 错误号(2位)
        self.message = ""

    def enter(self):
        """
        处理具体业务
        :return: 0/不回包给前端，pb/正确返回，timeout/超时
        """
        try:
            # 验证登录态，某些命令可能不需要登录态，此处做判断
            code, message = verify_session_key(self.numbers, self.session_key)
            if 10400 != code:
                g_log.debug("verify session key failed, %s, %s", code, message)
                return package.error_response(self.cmd, self.seq, 60001, "invalid session key")

            command_handle = {501: self.merchant_credit_flow_retrieve, 502: self.merchant_settlement,
                              503: self.merchant_recharge, 504: self.merchant_withdrawals,
                              505: self.balance_record_retrieve}

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
            from print_exception import print_exception
            print_exception()
            g_log.error("%s", e)
            return 0

    def merchant_credit_flow_retrieve(self):
        """
        读取商家积分、余额详情
        :return:
        """
        try:
            body = self.request.merchant_credit_flow_retrieve_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity
            identity = body.identity

            if not numbers:
                # 根据包体中的merchant_identity获取numbers
                code, numbers = identity_to_numbers(identity)
                if code != 10500:
                    self.code = 60101
                    self.message = "missing argument"
                    return 1

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

                credit_flow = response.merchant_credit_flow_retrieve_response.credit_flow
                last_merchant = ""
                # 遍历管理员所有商家
                for value in self.message:
                    if last_merchant != value["merchant_identity"]:
                        credit_flow_one = credit_flow.add()
                        # 商家资料
                        code, merchants = merchant_retrieve_with_merchant_identity_only(value["merchant_identity"])
                        merchant_material_copy_from_document(credit_flow_one.merchant, merchants[0])
                        material = credit_flow_one.material
                    # aggressive_record_one = aggressive_record.add()
                    last_merchant = value["merchant_identity"]
                    merchant_flow_copy_from_document(material, value)

                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    # deprecated
    def merchant_settlement(self):
        try:
            body = self.request.merchant_settlement_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity
            exec_settlement = body.exec_settlement
            identity = body.identity

            if not numbers:
                # 根据包体中的merchant_identity获取numbers
                code, numbers = identity_to_numbers(identity)
                if code != 10500:
                    self.code = 60201
                    self.message = "missing argument"
                    return 1

            kwargs = {"numbers": numbers, "merchant_identity": merchant_identity, "exec_settlement": exec_settlement}
            g_log.debug("merchant recharge: %s", kwargs)
            self.code, self.message = merchant_settlement(**kwargs)

            if 60200 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "merchant settlement done"

                response.merchant_settlement_response.settlement = self.message
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def merchant_recharge(self):
        """
        商家充值
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.merchant_recharge_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity
            money = body.money
            identity = body.identity

            if not numbers:
                # 根据包体中的identity获取numbers
                code, numbers = identity_to_numbers(identity)
                if code != 10500:
                    self.code = 60301
                    self.message = "missing argument"
                    return 1

            kwargs = {"numbers": numbers, "merchant_identity": merchant_identity, "money": money}
            g_log.debug("merchant recharge: %s", kwargs)
            self.code, self.message = merchant_recharge(**kwargs)

            if 60300 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "merchant recharge done"
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def merchant_withdrawals(self):
        pass

    def balance_record_retrieve(self):
        """
        读取充值、提现纪录
        :return:
        """
        try:
            body = self.request.merchant_balance_record_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity
            identity = body.identity

            if not numbers:
                # 根据包体中的identity获取numbers
                code, numbers = identity_to_numbers(identity)
                if code != 10500:
                    self.code = 50801
                    self.message = "missing argument"
                    return 1

            if merchant_identity:
                g_log.debug("%s retrieve merchant %s recharge record", numbers, merchant_identity)
            else:
                g_log.debug("%s retrieve all merchant recharge record", numbers)
            self.code, self.message = balance_record_retrieve(numbers, merchant_identity)

            if 50800 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "retrieve recharge record done"

                balance_record = response.merchant_balance_record_response.balance_record
                last_merchant = ""
                # 遍历管理员所有商家
                for value in self.message:
                    if last_merchant != value["merchant_identity"]:
                        balance_record_one = balance_record.add()
                        # 商家资料
                        code, merchants = merchant_retrieve_with_merchant_identity_only(value["merchant_identity"])
                        merchant_material_copy_from_document(balance_record_one.merchant, merchants[0])
                        aggressive_record = balance_record_one.aggressive_record
                    aggressive_record_one = aggressive_record.add()
                    last_merchant = value["merchant_identity"]
                    balance_record_copy_from_document(aggressive_record_one, value)

                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

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


def bond_to_upper_bound(bond, ratio):
    """
    根据保证金计算商家积分上限
    :param bond:
    :return:
    """
    # TODO... 保证金检查
    return bond * ratio


def upper_bound_update(**kwargs):
    """
    商家积分上限变更，保证金变更的时候调
    :param kwargs: {"numbers": 1000000, "merchant_identity": "", "bond": 1000}
    :return: (60100, "yes")/成功，(>60100, "errmsg")/失败
    """
    try:
        # 检查请求用户numbers必须是平台管理员
        numbers = kwargs.get("numbers", "")
        if not account_is_platform(numbers):
            g_log.warning("not platform %s", numbers)
            return 60311, "no privilege"

        # 必须是已认证商家，在更新保证金已经做过验证，此处省略
        merchant_identity = kwargs.get("merchant_identity", "")

        bond = kwargs.get("bond", 0)
        ratio = kwargs.get("ratio", 0)
        upper_bound = bond_to_upper_bound(bond, ratio)
        value = {"merchant_identity": merchant_identity, "upper_bound": upper_bound, "deleted": 0}

        # 存入数据库
        collection = get_mongo_collection("flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60313, "get collection flow failed"
        flow = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0}, {"$set": value})

        # 第一次更新，则插入一条
        if not flow:
            g_log.debug("insert new flow")
            flow = collection.insert_one(value)
        if not flow:
            g_log.error("update merchant %s credit upper bound failed", merchant_identity)
            return 60314, "update failed"
        g_log.debug("update upper bound succeed")

        # 更新记录入库
        collection = get_mongo_collection("flow_record")
        if not collection:
            g_log.error("get collection flow record failed")
            return 60315, "get collection flow record failed"
        quantization = "bond:%d, bound:%d" % (bond, upper_bound)
        result = collection.insert_one({"merchant_identity": merchant_identity, "time": datetime.now(),
                                        "operator": numbers, "quantization": quantization})
        if not result:
            g_log.error("insert flow record failed")
            # return 60316, "insert flow record failed"

        return 60300, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 60317, "exception"


def merchant_credit_update(**kwargs):
    """
    商家积分变更
    积分类型：可发行积分总量、已发行积分、积分互换IN & OUT、用户消费积分、账户余额变更
    mode=["may_issued", "issued", "interchange_in", "interchange_out", "consumption", "balance"]
    :param kwargs: {"numbers": 11868898224, "merchant_identity": "", "mode": may_issued, "supplement": 1000}
    :return:
    """
    try:
        # 检查请求用户numbers必须是平台管理员
        numbers = kwargs.get("numbers", "")
        if not account_is_valid_merchant(numbers):
            g_log.warning("not manager %s", numbers)
            return 60411, "no privilege"
        # 必须是已认证商家，在补充可发行积分总量时已经做过验证，此处省略

        merchant_identity = kwargs.get("merchant_identity", "")
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 60412, "not manager"
        merchant_founder = merchant["merchant_founder"]
        g_log.debug("merchant %s founder %s", merchant_identity, merchant_founder)

        mode = kwargs.get("mode", "")
        modes = ["may_issued", "issued", "interchange_in", "interchange_out", "consumption", "balance"]
        if mode not in modes:
            g_log.error("not supported mode %s", mode)
            return 60413, "not supported mode"
        # TODO... 积分检查
        supplement = kwargs.get("supplement", 0)
        value = {"merchant_identity": merchant_identity, mode: supplement, "deleted": 0}

        # 存入数据库
        collection = get_mongo_collection("flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60414, "get collection flow failed"
        flow = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
                                              {"$inc": {mode: supplement}})

        # 第一次更新，则插入一条
        if not flow:
            g_log.debug("insert new flow")
            flow = collection.insert_one(value)
        if not flow:
            g_log.error("update merchant %s %s credit failed", merchant_identity, mode)
            return 60415, "update failed"
        g_log.debug("update merchant %s credit succeed", mode)

        # 更新记录入库
        collection = get_mongo_collection("flow_record")
        if not collection:
            g_log.error("get collection flow record failed")
            return 60416, "get collection flow record failed"
        quantization = "mode:%s, supplement:%d" % (mode, supplement)
        result = collection.insert_one({"merchant_identity": merchant_identity, "time": datetime.now(),
                                        "operator": numbers, "quantization": quantization})
        if not result:
            g_log.error("insert flow record failed")

        return 60400, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 60417, "exception"


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
            return 60112, "not manager"

        collection = get_mongo_collection("flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60113, "get collection flow failed"
        records = collection.find({"merchant_identity": merchant_identity, "deleted": 0})
        if not records:
            g_log.error("retrieve flow failed")
            return 60114, "retrieve failed"

        return 60100, records
    except Exception as e:
        g_log.error("%s", e)
        return 60115, "exception"


def merchant_credit_flow_retrieve_all(numbers):
    """
    查找所有商家的操作纪录
    :param numbers:
    :return:
    """
    try:
        if not account_is_platform(numbers):
            g_log.error("%s not platform", numbers)
            return 60116, "no privilege"

        # 广播查找所有商家的积分详情
        collection = get_mongo_collection("flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60117, "get collection flow failed"
        records = collection.find({"deleted": 0}).sort("merchant_identity")
        if not records:
            g_log.error("retrieve flow failed")

        return 60100, records
    except Exception as e:
        g_log.error("%s", e)
        return 60118, "exception"


# deprecated
def merchant_settlement(**kwargs):
    """
    计算商家可结算的积分
    :param numbers: 平台账号或管理员账号
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        # 检查要创建者numbers
        numbers = kwargs.get("numbers", "")
        merchant_identity = kwargs.get("merchant_identity")

        # 检查管理员和商家关系
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 60212, "not manager"

        collection = get_mongo_collection("flow")
        if not collection:
            g_log.error("get collection flow failed")
            return 60213, "get collection flow failed"
        record = collection.find_one({"merchant_identity": merchant_identity, "deleted": 0})
        if not record:
            g_log.error("retrieve flow failed")
            return 60214, "retrieve failed"
        settlement = record["interchange_in"] - record["interchange_out"] - record["settlement"]

        exec_settlement = kwargs.get("exec_settlement", 0)
        if not exec_settlement:
            # TODO... 换成钱
            return 60200, settlement

        # TODO... 积分结算
        flow = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
                                              {"$set": {"settlement": settlement}})
        if not flow:
            g_log.error("merchant exec settlement %s failed", merchant_identity)
            return 60215, "settlement failed"
        g_log.debug("settlement done")

        # 更新记录入库
        collection = get_mongo_collection("flow_record")
        if not collection:
            g_log.error("get collection flow record failed")
            return 60216, "get collection flow record failed"
        quantization = "in:%d, out:%d, last_settlement:%d, settlement:%d" % \
                       (record["interchange_in"], record["interchange_out"], record["settlement"], settlement)
        result = collection.insert_one({"merchant_identity": merchant_identity, "time": datetime.now(),
                                        "operator": numbers, "quantization": quantization})
        if not result:
            g_log.error("insert flow record failed")

        return 60200, settlement
    except Exception as e:
        g_log.error("%s", e)
        return 60217, "exception"


# pragma 商家充值API
def merchant_recharge(**kwargs):
    """
    更新消费换积分比率，未认证商家不允许操作
    :param kwargs: {"numbers": 118688982240, "merchant_identity": "", "money": 100}
    :return: (60300, "yes")/成功，(>60300, "errmsg")/失败
    """
    try:
        # 检查要请求用户numbers必须是平台管理员
        numbers = kwargs.get("numbers", "")
        if not account_is_valid_merchant(numbers):
            g_log.warning("not manager %s", numbers)
            return 60311, "not manager"

        # 检查管理员和商家关系
        merchant_identity = kwargs.get("merchant_identity", "")
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 60312, "not manager"
        # merchant_founder = merchant["merchant_founder"]
        # g_log.debug("merchant %s founder %s", merchant_identity, merchant_founder)

        # 认证用户才可以充值
        if not merchant_is_verified(merchant_identity):
            g_log.error("merchant %s not verified", merchant_identity)
            return 60318, "not verified"

        # TODO...充值金额检查
        money = kwargs.get("money", 0)

        # 存入数据库
        code, message = merchant_credit_update(**{"numbers": numbers, "merchant_identity": merchant_identity,
                                                  "mode": "recharge", "supplement": money})
        # collection = get_mongo_collection("flow")
        # if not collection:
        #     g_log.error("get collection parameters failed")
        #     return 60313, "get collection parameters failed"
        #
        # business_parameters = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
        #                                                      {"$inc": {"balance": money}},
        #                                                      return_document=ReturnDocument.AFTER)
        # if not business_parameters:
        #     g_log.error("update merchant %s parameters failed", merchant_identity)
        #     return 60314, "update failed"
        # g_log.debug("recharge done, money: %s", business_parameters["balance"])

        # 更新记录入库
        collection = get_mongo_collection("balance_record")
        if not collection:
            g_log.error("get collection balance record failed")
            return 60315, "get collection balance record failed"
        result = collection.insert_one({"merchant_identity": merchant_identity, "time": datetime.now(),
                                        "operator": numbers, "money": money, "type": "recharge"})
        if not result:
            g_log.error("insert recharge record failed")
            return 60316, "insert recharge record failed"
        return 60300, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 60317, "exception"


def balance_record_retrieve(numbers, merchant_identity):
    """
    读取充值、提现纪录，没给出merchant_identity则读取全部
    :param numbers: 平台账号或管理员账号
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        if not merchant_identity:
            # 平台读取所有操作纪录
            return balance_record_retrieve_all(numbers)

        # 检查管理员和商家关系
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 60512, "not manager"

        # 广播查找所有商家的经营参数更新纪录
        collection = get_mongo_collection("balance_record")
        if not collection:
            g_log.error("get collection balance record failed")
            return 60513, "get collection balance record failed"
        records = collection.find({"merchant_identity": merchant_identity})
        if not records:
            g_log.error("retrieve balance record failed")

        return 60500, records
    except Exception as e:
        g_log.error("%s", e)
        return 60514, "exception"


def balance_record_retrieve_all(numbers):
    """
    查找所有商家的充值操作纪录
    :param numbers:
    :return:
    """
    try:
        if not account_is_platform(numbers):
            g_log.error("%s not platform", numbers)
            return 60515, "no privilege"

        # 广播查找所有商家的经营参数更新纪录
        collection = get_mongo_collection("balance_record")
        if not collection:
            g_log.error("get collection balance record failed")
            return 60516, "get collection balance record failed"
        records = collection.find({}).sort("merchant_identity")
        if not records:
            g_log.error("retrieve balance record failed")

        return 60500, records
    except Exception as e:
        g_log.error("%s", e)
        return 60517, "exception"


def merchant_flow_copy_from_document(material, value):
    g_log.debug("yes")
    g_log.debug(value["may_issued"])
    material.upper_bound = int(value["upper_bound"])
    material.may_issued = int(value["may_issued"])
    material.issued = int(value["issued"])
    material.interchange_in = int(value["interchange_in"])
    material.interchange_out = int(value["interchange_out"])
    # material.interchange_consumption = int(value["interchange_consumption"])
    material.consumption = int(value["consumption"])
    material.balance = int(value["balance"])
    material.identity = str(value["_id"])


def balance_record_copy_from_document(material, value):
    material.operator = value["operator"]
    material.time = value["time"].strftime("%Y-%m-%d %H:%M:%S")
    material.money = value["money"]
    material.identity = str(value["_id"])
    material.direction = value["direction"]


# 测试时mongo_connection的配置文件路径写全
if "__main__" == __name__:
    kwargs1 = {"numbers": "118688982240", "merchant_identity": "562c7ad6494ac55faf750798", "bond": 100}
    upper_bound_update(**kwargs1)
    kwargs1 = {"numbers": "118688982240", "merchant_identity": "562c7ad6494ac55faf750798", "supplement": 1002}
    for mode1 in ["may_issued", "issued", "interchange_in", "interchange_out", "interchange_consumption"]:
        kwargs1["mode"] = mode1
        merchant_credit_update(**kwargs1)
