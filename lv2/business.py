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


class Business():
    """
    商家经营参数模块，命令号<500
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
            command_handle = {401: self.platform_update_parameters, 402: self.business_parameters_retrieve,
                              403: self.business_parameters_batch_retrieve,
                              404: self.consumption_ratio_update, 405: self.dummy_command,
                              406: self.parameters_record_retrieve, 407: self.merchant_recharge,
                              408: self.recharge_record_retrieve}
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

    def platform_update_parameters(self):
        """
        平台修改商家保证金、账户余额换积分比率
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.platform_update_parameters_request
            numbers = body.numbers
            manager = body.manager
            merchant_identity = body.merchant_identity
            bond = body.bond
            ratio = body.balance_ratio

            # TODO... numbers必须是平台管理员

            kwargs = {"numbers": numbers, "merchant_identity": merchant_identity, "manager": manager,
                      "bond": bond, "balance_ratio": ratio}
            g_log.debug("platform update parameters: %s", kwargs)
            self.code, self.message = platform_update_parameters(**kwargs)

            if 50100 == self.code:
                # 成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "add business parameters done"
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def business_parameters_retrieve(self):
        """
        获取商家经营参数
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.business_parameters_retrieve_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity

            if not numbers:
                # TODO... 根据包体中的identity获取numbers
                pass

            # 发起请求的用户和要获取的用户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to retrieve business parameters %s", self.numbers, numbers)
                self.code = 50208
                self.message = "no privilege to retrieve business parameters"
                return 1

            self.code, self.message = business_parameters_retrieve_with_numbers(numbers, merchant_identity)

            if 50200 == self.code:
                # 获取成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "retrieve business parameters done"

                material = response.business_parameters_retrieve_response.material
                business_parameters_material_copy_from_document(material, self.message)
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def business_parameters_batch_retrieve(self):
        pass

    def consumption_ratio_update(self):
        """
        更新消费换积分比率
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.consumption_ratio_update_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity
            ratio = body.consumption_ratio

            if not numbers:
                # TODO... 根据包体中的identity获取numbers
                pass

            kwargs = {"numbers": numbers, "merchant_identity": merchant_identity, "consumption_ratio": ratio}
            g_log.debug("update consumption ratio: %s", kwargs)
            self.code, self.message = consumption_ratio_update(**kwargs)

            if 50400 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "update consumption ratio done"
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def parameters_record_retrieve(self):
        """
        读取经营参数变更纪录
        :return:
        """
        try:
            body = self.request.parameters_record_retrieve_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity

            if not numbers:
                # TODO... 根据包体中的identity获取numbers
                pass

            if merchant_identity:
                g_log.debug("%s retrieve merchant %s parameters", numbers, merchant_identity)
            else:
                g_log.debug("%s retrieve all merchant parameters", numbers)
            self.code, self.message = parameters_record_retrieve(numbers, merchant_identity)

            if 50600 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "retrieve record done"

                parameters_record = response.parameters_record_retrieve_response.parameters_record
                last_merchant = ""
                # 遍历管理员所有商家
                for value in self.message:
                    if last_merchant != value["merchant_identity"]:
                        parameters_record_one = parameters_record.add()
                        # 商家资料
                        code, merchants = merchant_retrieve_with_merchant_identity_only(value["merchant_identity"])
                        merchant_material_copy_from_document(parameters_record_one.merchant, merchants[0])
                        aggressive_record = parameters_record_one.aggressive_record
                    aggressive_record_one = aggressive_record.add()
                    last_merchant = value["merchant_identity"]
                    business_parameters_record_copy_from_document(aggressive_record_one, value)

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

            if not numbers:
                # TODO... 根据包体中的identity获取numbers
                pass

            kwargs = {"numbers": numbers, "merchant_identity": merchant_identity, "money": money}
            g_log.debug("merchant recharge: %s", kwargs)
            self.code, self.message = merchant_recharge(**kwargs)

            if 50700 == self.code:
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

    def recharge_record_retrieve(self):
        """
        读取充值纪录
        :return:
        """
        try:
            body = self.request.merchant_recharge_record_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity

            if not numbers:
                # TODO... 根据包体中的identity获取numbers
                pass

            if merchant_identity:
                g_log.debug("%s retrieve merchant %s recharge record", numbers, merchant_identity)
            else:
                g_log.debug("%s retrieve all merchant recharge record", numbers)
            self.code, self.message = recharge_record_retrieve(numbers, merchant_identity)

            if 50800 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "retrieve recharge record done"

                recharge_record = response.merchant_recharge_record_response.recharge_record
                last_merchant = ""
                # 遍历管理员所有商家
                for value in self.message:
                    if last_merchant != value["merchant_identity"]:
                        recharge_record_one = recharge_record.add()
                        # 商家资料
                        code, merchants = merchant_retrieve_with_merchant_identity_only(value["merchant_identity"])
                        merchant_material_copy_from_document(recharge_record_one.merchant, merchants[0])
                        aggressive_record = recharge_record_one.aggressive_record
                    aggressive_record_one = aggressive_record.add()
                    last_merchant = value["merchant_identity"]
                    recharge_record_copy_from_document(aggressive_record_one, value)

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
        business = Business(request)
        return business.enter()
    except Exception as e:
        g_log.error("%s", e)
        return 0
    
    
# pragma 平台修改商家保证金、账户余额换积分比率API
def platform_update_parameters(**kwargs):
    """
    平台修改商家保证金、账户余额换积分比率
    :param kwargs: {"numbers": 1000000, "merchant_identity": "", "manager": 11868898224,
                    "bond": 1000, "balance_ratio": 1}
    :return: (50100, "yes")/成功，(>50100, "errmsg")/失败
    """
    try:
        # 检查请求用户numbers必须是平台管理员
        numbers = kwargs.get("numbers", "")
        if not account_is_platform(numbers):
            g_log.warning("not platform %s", numbers)
            return 50101, "no privilege"

        # 必须是已认证商家
        manager = kwargs.get("manager", "")
        merchant_identity = kwargs.get("merchant_identity", "")
        merchant = user_is_merchant_manager(manager, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", manager, merchant_identity)
            return 50102, "not manager"
        merchant_founder = merchant["merchant_founder"]
        if not merchant_is_verified(merchant_founder, merchant_identity):
            g_log.error("merchant %s not verified", merchant_identity)
            return 50108, "not verified"
        g_log.debug("merchant %s founder %s", merchant_identity, merchant_founder)

        # TODO... 保证金、账户余额、账户余额换积分比率、消费换积分比率检查
        bond = kwargs.get("bond", 0)
        balance_ratio = kwargs.get("balance_ratio", 0)
        value = {"merchant_identity": merchant_identity, "bond": bond, "balance_ratio": balance_ratio, "deleted": 0}

        # 存入数据库
        collection = get_mongo_collection("parameters")
        if not collection:
            g_log.error("get collection parameters failed")
            return 50103, "get collection parameters failed"
        business_parameters = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
                                                             {"$set": value})

        # 第一次更新，则插入一条
        if not business_parameters:
            g_log.debug("insert new parameters")
            value["balance"] = 0
            value["consumption_ratio"] = 0
            business_parameters = collection.insert_one(value)

        if not business_parameters:
            g_log.error("update merchant %s parameters failed", merchant_identity)
            return 50104, "update failed"
        g_log.debug("update parameter succeed")

        # 更新记录入库
        collection = get_mongo_collection("parameters_record")
        if not collection:
            g_log.error("get collection parameters record failed")
            return 50105, "get collection parameters record failed"
        quantization = "bond:%s, balance_ratio:%s" % (bond, balance_ratio)
        result = collection.insert_one({"merchant_identity": merchant_identity, "time": datetime.now(),
                                        "operator": numbers, "quantization": quantization})
        if not result:
            g_log.error("insert parameters record failed")
            return 50106, "insert parameters record failed"
        return 50100, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 50107, "exception"


# pragma 读取商家经营参数API
def business_parameters_retrieve_with_numbers(numbers, merchant_identity):
    """
    读取商家经营参数
    :param numbers: 管理员电话号码
    :param merchant_identity: 商家ID
    :return: (50200, parameters)/成功，(>50200, "errmsg")/失败
    """
    try:
        # 检查管理员和商家关系
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.warning("invalid manager account %s", numbers)
            return 50201, "invalid manager"
        founder = merchant["merchant_founder"]
        g_log.debug("merchant %s founder %s", merchant_identity, founder)

        collection = get_mongo_collection("parameters")
        if not collection:
            g_log.error("get collection parameters failed")
            return 50202, "get collection parameters failed"
        business_parameters = collection.find_one({"merchant_identity": merchant_identity, "deleted": 0})
        if not business_parameters:
            g_log.debug("merchant %s parameters not exist", merchant_identity)
            return 50203, "parameters not exist"
        return 50200, business_parameters
    except Exception as e:
        g_log.error("%s", e)
        return 50204, "exception"


def business_parameters_retrieve_with_identity(identity, merchant_identity):
    """
    读取商家经营参数
    :param identity: 管理员ID
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        # 根据id查找电话号码
        numbers = ""
        return business_parameters_retrieve_with_numbers(numbers, merchant_identity)
    except Exception as e:
        g_log.error("%s", e)
        return 50205, "exception"


def business_parameters_retrieve(merchant_identity, numbers=None, identity=None):
    """
    读取商家经营参数，电话号码优先
    :param numbers: 管理员电话号码
    :param identity: 管理员ID
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        if numbers:
            return business_parameters_retrieve_with_numbers(numbers, merchant_identity)
        elif identity:
            return business_parameters_retrieve_with_identity(identity, merchant_identity)
        else:
            return 50206, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 50207, "exception"


# pragma 更新消费换积分比率API
def consumption_ratio_update(**kwargs):
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
            return 50401, "not manager"

        # 检查管理员和商家关系
        merchant_identity = kwargs.get("merchant_identity", "")
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 50402, "not manager"
        merchant_founder = merchant["merchant_founder"]
        g_log.debug("merchant %s founder %s", merchant_identity, merchant_founder)

        # if not merchant_is_verified(merchant_founder, merchant_identity):
        #     g_log.error("merchant %s not verified", merchant_identity)
        #     return 50403, "not verified"

        # TODO... 消费换积分比率检查
        consumption_ratio = kwargs.get("consumption_ratio", 0)
        value = {"merchant_identity": merchant_identity, "consumption_ratio": consumption_ratio, "deleted": 0}

        # 存入数据库
        collection = get_mongo_collection("parameters")
        if not collection:
            g_log.error("get collection parameters failed")
            return 50403, "get collection parameters failed"
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
            return 50404, "update failed"
        g_log.debug("update consumption done")

        # 更新记录入库
        collection = get_mongo_collection("parameters_record")
        if not collection:
            g_log.error("get collection parameters record failed")
            return 50405, "get collection parameters record failed"
        quantization = "consumption_ratio:%s" % (consumption_ratio,)
        result = collection.insert_one({"merchant_identity": merchant_identity, "time": datetime.now(),
                                        "operator": numbers, "quantization": quantization})
        if not result:
            g_log.error("insert parameters record failed")
            return 50406, "insert parameters record failed"
        return 50400, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 50407, "exception"


# pragma 删除用户资料API
def business_parameters_delete_with_numbers(numbers, merchant_identity):
    """
    删除商家经营参数
    :param numbers: 管理员电话号码
    :param merchant_identity: 商家ID
    :return: (50500, "yes")/成功，(>50500, "errmsg")/失败
    """
    try:
        # 检查合法账号
        # if not account_is_valid_merchant(numbers):
        #     g_log.warning("invalid customer account %s", numbers)
        #     return 50501, "invalid phone number"

        # 检查管理员和商家关系
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 50501, "not manager"
        merchant_founder = merchant["merchant_founder"]
        g_log.debug("merchant %s founder %s", merchant_identity, merchant_founder)

        collection = get_mongo_collection("parameters")
        if not collection:
            g_log.error("get collection business_parameters failed")
            return 50502, "get collection business_parameters failed"
        business_parameters = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
                                                             {"$set": {"deleted": 1}})
        if not business_parameters:
            g_log.error("merchant %s parameters not exist", merchant_identity)
            return 50503, "merchant parameters not exist"

        # 更新记录入库
        collection = get_mongo_collection("parameters_record")
        if not collection:
            g_log.error("get collection parameters record failed")
            return 50504, "get collection parameters record failed"
        quantization = "deleted:1"
        result = collection.insert_one({"merchant_identity": merchant_identity, "time": datetime.now(),
                                        "operator": numbers, "quantization": quantization})
        if not result:
            g_log.error("insert parameters record failed")
            return 50505, "insert parameters record failed"

        return 50500, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 50506, "exception"


def business_parameters_delete_with_identity(identity, merchant_identity):
    """
    删除商家经营参数
    :param identity: 管理员ID
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        # 根据用户id查找用户电话号码
        numbers = ""
        return business_parameters_delete_with_numbers(numbers, merchant_identity)
    except Exception as e:
        g_log.error("%s", e)
        return 50507, "exception"


def business_parameters_delete(merchant_identity, numbers=None, identity=None):
    """
    删除商家经营参数，电话号码优先
    :param numbers: 管理员电话号码
    :param identity: 管理员ID
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        if numbers:
            return business_parameters_delete_with_numbers(numbers, merchant_identity)
        elif identity:
            return business_parameters_delete_with_identity(identity, merchant_identity)
        else:
            return 50508, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 50509, "exception"


def business_parameters_delete_by_platform(numbers, merchant_identity):
    """
    平台删除商家经营参数
    :param numbers: 平台账号
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        # 检查合法账号
        if not account_is_valid_merchant(numbers):
            g_log.warning("invalid customer account %s", numbers)
            return 50510, "invalid phone number"

        collection = get_mongo_collection("parameters")
        if not collection:
            g_log.error("get collection business parameters failed")
            return 50511, "get collection business parameters failed"
        business_parameters = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
                                                             {"$set": {"deleted": 1}})
        if not business_parameters:
            g_log.error("merchant %s parameters not exist", merchant_identity)
            return 50512, "merchant parameters not exist"

        # 更新记录入库
        collection = get_mongo_collection("parameters_record")
        if not collection:
            g_log.error("get collection parameters record failed")
            return 50513, "get collection parameters record failed"
        quantization = "deleted:1"
        result = collection.insert_one({"merchant_identity": merchant_identity, "time": datetime.now(),
                                        "operator": numbers, "quantization": quantization})
        if not result:
            g_log.error("insert parameters record failed")
            return 50514, "insert parameters record failed"

        return 50500, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 50515, "exception"


def parameters_record_retrieve(numbers, merchant_identity):
    """
    读取商家经营参数，没给出merchant_identity则读取全部
    :param numbers: 平台账号或管理员账号
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        if not merchant_identity:
            # 平台读取所有操作纪录
            return parameters_record_retrieve_all(numbers)

        # 检查管理员和商家关系
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 50602, "not manager"

        # 广播查找所有商家的经营参数更新纪录
        collection = get_mongo_collection("parameters_record")
        if not collection:
            g_log.error("get collection parameters record failed")
            return 50603, "get collection parameters record failed"
        records = collection.find({"merchant_identity": merchant_identity})
        if not records:
            g_log.error("insert parameters record failed")

        return 50600, records
    except Exception as e:
        g_log.error("%s", e)
        return 50604, "exception"


def parameters_record_retrieve_all(numbers):
    """
    查找所有商家的操作纪录
    :param numbers:
    :return:
    """
    try:
        if not account_is_platform(numbers):
            g_log.error("%s not platform", numbers)
            return 50605, "no privilege"

        # 广播查找所有商家的经营参数更新纪录
        collection = get_mongo_collection("parameters_record")
        if not collection:
            g_log.error("get collection parameters record failed")
            return 50606, "get collection parameters record failed"
        records = collection.find({}).sort("merchant_identity")
        if not records:
            g_log.error("retrieve parameters record failed")

        return 50600, records
    except Exception as e:
        g_log.error("%s", e)
        return 50607, "exception"


# pragma 商家充值API
def merchant_recharge(**kwargs):
    """
    更新消费换积分比率，未认证商家不允许操作
    :param kwargs: {"numbers": 118688982240, "merchant_identity": "", "money": 100}
    :return: (50700, "yes")/成功，(>50700, "errmsg")/失败
    """
    try:
        # 检查要请求用户numbers必须是平台管理员
        numbers = kwargs.get("numbers", "")
        if not account_is_valid_merchant(numbers):
            g_log.warning("not manager %s", numbers)
            return 50701, "not manager"

        # 检查管理员和商家关系
        merchant_identity = kwargs.get("merchant_identity", "")
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 50702, "not manager"
        merchant_founder = merchant["merchant_founder"]
        g_log.debug("merchant %s founder %s", merchant_identity, merchant_founder)

        # 认证用户才可以充值
        if not merchant_is_verified(merchant_founder, merchant_identity):
            g_log.error("merchant %s not verified", merchant_identity)
            return 50708, "not verified"

        # TODO...充值金额检查
        money = kwargs.get("money", 0)

        # 存入数据库
        collection = get_mongo_collection("parameters")
        if not collection:
            g_log.error("get collection parameters failed")
            return 50703, "get collection parameters failed"

        business_parameters = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
                                                             {"$inc": {"balance": money}},
                                                             return_document=ReturnDocument.AFTER)
        if not business_parameters:
            g_log.error("update merchant %s parameters failed", merchant_identity)
            return 50704, "update failed"
        g_log.debug("recharge done, money: %s", business_parameters["balance"])

        # 更新记录入库
        collection = get_mongo_collection("recharge_record")
        if not collection:
            g_log.error("get collection recharge record failed")
            return 50705, "get collection recharge record failed"
        result = collection.insert_one({"merchant_identity": merchant_identity, "time": datetime.now(),
                                        "operator": numbers, "money": money})
        if not result:
            g_log.error("insert recharge record failed")
            return 50706, "insert recharge record failed"
        return 50700, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 50707, "exception"


def recharge_record_retrieve(numbers, merchant_identity):
    """
    读取充值纪录，没给出merchant_identity则读取全部
    :param numbers: 平台账号或管理员账号
    :param merchant_identity: 商家ID
    :return:
    """
    try:
        if not merchant_identity:
            # 平台读取所有操作纪录
            return recharge_record_retrieve_all(numbers)

        # 检查管理员和商家关系
        merchant = user_is_merchant_manager(numbers, merchant_identity)
        if not merchant:
            g_log.error("%s is not merchant %s manager", numbers, merchant_identity)
            return 50802, "not manager"

        # 广播查找所有商家的经营参数更新纪录
        collection = get_mongo_collection("recharge_record")
        if not collection:
            g_log.error("get collection recharge record failed")
            return 50803, "get collection recharge record failed"
        records = collection.find({"merchant_identity": merchant_identity})
        if not records:
            g_log.error("retrieve recharge record failed")

        return 50800, records
    except Exception as e:
        g_log.error("%s", e)
        return 50804, "exception"


def recharge_record_retrieve_all(numbers):
    """
    查找所有商家的充值操作纪录
    :param numbers:
    :return:
    """
    try:
        if not account_is_platform(numbers):
            g_log.error("%s not platform", numbers)
            return 50805, "no privilege"

        # 广播查找所有商家的经营参数更新纪录
        collection = get_mongo_collection("recharge_record")
        if not collection:
            g_log.error("get collection recharge record failed")
            return 50806, "get collection recharge record failed"
        records = collection.find({}).sort("merchant_identity")
        if not records:
            g_log.error("retrieve recharge record failed")

        return 50800, records
    except Exception as e:
        g_log.error("%s", e)
        return 50807, "exception"


def business_parameters_material_copy_from_document(material, value):
    material.bond = int(value["bond"])
    material.balance = int(value["balance"])
    material.balance_ratio = int(value["balance_ratio"])
    material.consumption_ratio = int(value["consumption_ratio"])
    material.identity = str(value["_id"])


def business_parameters_record_copy_from_document(material, value):
    material.operator = value["operator"]
    material.time = value["time"].strftime("%Y-%m-%d %H:%M:%S")
    material.quantization = value["quantization"]
    material.identity = str(value["_id"])


def recharge_record_copy_from_document(material, value):
    material.operator = value["operator"]
    material.time = value["time"].strftime("%Y-%m-%d %H:%M:%S")
    material.money = value["money"]
    material.identity = str(value["_id"])