# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import string
import random
import pymongo
from bson.objectid import ObjectId
import common_pb2
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
import package
from mongo_connection import get_mongo_connection, get_mongo_collection
from account_valid import user_is_valid_merchant, phone_number_is_valid, yes_no_2_char, char_2_yes_no


class Merchant():
    """
    注册登录模块，命令号<100
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
            command_handle = {200: self.merchant_create, 201: self.merchant_retrieve, 202: self.merchant_batch_retrieve,
                              203: self.merchant_update, 204: self.merchant_delete}
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

    def merchant_create(self):
        """
        创建merchant资料
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的merchant
        4 merchant写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.merchant_create_request
            numbers = body.numbers
            material = body.material

            if not numbers:
                if not material.numbers:
                    # TODO... 根据包体中的identity获取numbers
                    pass
                else:
                    numbers = material.numbers

            # 发起请求的商户和要创建的商户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to create merchant %s", self.numbers, numbers)
                self.code = 30106
                self.message = "no privilege to create merchant"
                return 1

            kwargs = {"numbers": numbers, "name": material.name, "name_en": material.name_en,
                      "introduce": material.introduce, "logo": material.logo, "email": material.email,
                      "country": material.country, "location": material.location, "qrcode": material.qrcode,
                      "latitude": material.latitude, "longitude": material.longitude, "verified": material.verified,
                      "contact_numbers": material.contact_numbers, "contract": material.contract}
            g_log.debug("create merchant: %s", kwargs)
            self.code, self.message = merchant_create(**kwargs)

            if 30100 == self.code:
                # 创建成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "create merchant done"

                response.merchant_create_response.merchant_identity = self.message
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def merchant_retrieve(self):
        """
        获取merchant资料
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的merchant
        4 merchant写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.merchant_retrieve_request
            numbers = body.numbers
            identity = body.identity

            if not numbers:
                # TODO... 根据包体中的merchant_identity获取numbers
                pass

            # 发起请求的商户和要获取的商户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to retrieve merchant %s", self.numbers, numbers)
                self.code = 30208
                self.message = "no privilege to retrieve merchant"
                return 1

            self.code, self.message = merchant_retrieve_with_numbers(numbers)

            if 30200 == self.code:
                # 获取成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "delete merchant done"

                value = self.message
                material = response.merchant_retrieve_response.material
                material.numbers = numbers
                material.introduce = value["introduce"]
                material.merchant_name = value["name"]
                material.logo = value["logo"]
                material.email = value["email"]
                material.location = value["location"]
                material.country = value["country"]
                material.latitude = float(value["latitude"])
                material.longitude = float(value["longitude"])
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def merchant_batch_retrieve(self):
        pass

    def merchant_update(self):
        """
        创建merchant资料
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的merchant
        4 merchant写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.merchant_create_request
            numbers = body.numbers
            material = body.material

            if not numbers:
                if not material.numbers:
                    # TODO... 根据包体中的merchant_identity获取numbers
                    pass
                else:
                    numbers = material.numbers

            # 发起请求的商户和要创建的商户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to update merchant %s", self.numbers, numbers)
                self.code = 30400
                self.message = "no privilege to update merchant"
                return 1

            kwargs = {"numbers": numbers, "merchant_name": material.merchat_name,
                      "introduce": material.introduce, "logo": material.logo, "email": material.email,
                      "country": material.country, "location": material.location,
                      "latitude": material.latitude, "longitude": material.longitude}
            g_log.debug("create merchant: %s", kwargs)
            self.code, self.message = merchant_update(kwargs)

            if 30400 == self.code:
                # 创建成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "create merchant done"
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def merchant_delete(self):
        """
        删除merchant资料
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的merchant
        4 merchant写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.merchant_delete_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity

            if not numbers:
                # TODO... 根据包体中的merchant_identity获取numbers
                pass

            # 发起请求的商户和要创建的商户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to delete merchant %s", self.numbers, numbers)
                self.code = 30510
                self.message = "no privilege to delete merchant"
                return 1

            self.code, self.message = merchant_delete_with_numbers(numbers)

            if 30500 == self.code:
                # 删除成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "delete merchant done"
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
    登录模块入口
    :param request: 解析后的pb格式
    :return: 0/不回包给前端，pb/正确返回，timeout/超时
    """
    try:
        merchant = Merchant(request)
        return merchant.enter()
    except Exception as e:
        g_log.error("%s", e)
        return 0


# pragma 增加商户资料API
def merchant_create(**kwargs):
    """
    增加商户资料
    :param kwargs: {"numbers": "18688982240", "name": "星巴克", "name_en": "Star Bucks", "introduce": "We sell coffee",
                    "logo": "", "email": "vip@starbucks.com", "country": "USA", "location": "california", "qrcode": "",
                    "contact_numbers", "021-88888888", "latitude": 38.38, "longitude": -114.8, "contract": "", 
                    "verified": "no"}
    :return: (30100, material_identity)/成功，(>30100, "errmsg")/失败
    """
    try:
        # 检查要创建者numbers
        numbers = kwargs.get("numbers", "")
        if not user_is_valid_merchant(numbers):
            g_log.warning("invalid customer account %s", numbers)
            return 30101, "invalid phone number"

        # 商户名称不能超过64字节，超过要截取前64字节
        name = kwargs.get("name")
        if not name:
            g_log.error("lost merchant name")
            return 30102, "illegal argument"

        if len(name) > 64:
            g_log.warning("too long merchant name %s", name)
            name = name[0:64]

        name_en = kwargs.get("name_en")
        if len(name_en) > 64:
            g_log.warning("too long merchant english name %s", name_en)
            name_en = name_en[0:64]

        verified = yes_no_2_char(kwargs.get("verified", "no"))

        # 商户介绍不能超过512字节，超过要截取前512字节
        introduce = kwargs.get("introduce", "")
        if len(introduce) > 512:
            g_log.warning("too long introduce %s", introduce)
            introduce = introduce[0:512]
            
        contact_numbers = kwargs.get("contact_numbers")
        if not phone_number_is_valid(contact_numbers):
            contact_numbers = numbers
            
        # TODO... qrcode、email、logo、国家、地区、合同编号检查
        logo = kwargs.get("logo", "")
        email = kwargs.get("email", "")
        country = kwargs.get("country", "")
        location = kwargs.get("location", "")
        qrcode = kwargs.get("qrcode", "")
        contract = kwargs.get("contract", "")

        latitude = kwargs.get("latitude", 0)
        if latitude < -90 or latitude > 90:
            g_log.warning("latitude illegal, %s", latitude)
            latitude = 0
        longitude = kwargs.get("longitude", 0)
        if longitude < -180 or longitude > 180:
            g_log.warning("longitude illegal, %s", longitude)
            longitude = 0

        value = {"name": name, "name_en": name_en, "verified": verified, "logo": logo, "email": email,
                 "introduce": introduce, "latitude": latitude, "qrcode": qrcode, "contract": contract,
                 "longitude": longitude, "country": country, "location": location, 
                 "contact_numbers": contact_numbers, "deleted": 0}
        
        # 创建商户资料，商户和资料关联，事务
        collection = get_mongo_collection(numbers, "merchant")
        if not collection:
            g_log.error("get collection merchant failed")
            return 30103, "get collection merchant failed"
        merchant_identity = collection.insert_one(value).inserted_id
        merchant_identity = str(merchant_identity)
        g_log.debug("insert merchant %s", value)

        collection = get_mongo_collection(numbers, "number_merchant")
        if not collection:
            g_log.error("get collection number_merchant failed")
            return 30104, "get collection number_merchant failed"
        collection.insert_one({"numbers": numbers, "merchant_identity": merchant_identity, "deleted": 0})
        g_log.debug("insert merchant numbers many-many relation, %s:%s", merchant_identity, numbers)

        return 30100, merchant_identity
    # except (mongo.ConnectionError, mongo.TimeoutError) as e:
    #     g_log.error("connect to mongo failed")
    #     return 30102, "connect to mongo failed"
    except Exception as e:
        g_log.error("%s %s", e.__class__, e)
        return 30105, "exception"


# pragma 读取商户资料API
def merchant_retrieve_with_numbers(numbers):
    """
    读取商户资料
    :param numbers: 商户电话号码
    :return: (30200, "yes")/成功，(>30200, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_merchant(numbers):
            g_log.warning("invalid merchant account %s", numbers)
            return 30201, "invalid phone number"

        # 获取用户拥有的所有商户ID
        collection = get_mongo_collection(numbers, "number_merchant")
        if not collection:
            g_log.error("get collection number_merchant failed")
            return 30202, "get collection number_merchant failed"
        merchants = collection.find({"numbers": numbers, "deleted": 0}, {"merchant_identity": 1, "_id": 0})
        # g_log.debug(merchants.__class__)
        identities = []
        for merchant in merchants:
            # g_log.debug(merchant["merchant_identity"])
            identities.append(ObjectId(merchant["merchant_identity"]))
        g_log.debug(identities)

        collection = get_mongo_collection(numbers, "merchant")
        if not collection:
            g_log.error("get collection merchant failed")
            return 30202, "get collection merchant failed"
        merchants = collection.find({"_id": {"$in": identities}}).toArray()
        g_log.debug(merchants)
        for merchant in merchants:
            g_log.debug(merchant["name"])
        # if not merchant or merchant["deleted"] == 1:
        #     g_log.warning("merchant %s not exist", merchant_identity)
        #     return 30203, "merchant not exist"
        # g_log.debug("get %s %s", merchant_identity, merchant)
        # return 30200, merchant
        return 30220, "TODO..."
    except Exception as e:
        g_log.error("%s", e)
        return 30204, "exception"


def merchant_retrieve_with_identity(merchant_identity):
    """
    查询商户资料
    :param merchant_identity: 商户ID
    :return:
    """
    try:
        # 根据商户id查找商户电话号码
        numbers = ""
        return merchant_retrieve_with_numbers(numbers)
    except Exception as e:
        g_log.error("%s", e)
        return 30205, "exception"


def merchant_retrieve(numbers=None, merchant_identity=None):
    """
    获取商户资料，商户电话号码优先
    :param numbers: 商户电话号码
    :param merchant_identity: 商户ID
    :return:
    """
    try:
        if numbers:
            return merchant_retrieve_with_numbers(numbers)
        elif merchant_identity:
            return merchant_retrieve_with_identity(merchant_identity)
        else:
            return 30206, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 30207, "exception"


def merchant_retrieve_with_merchant_identity(merchant_identity):
    """
    获取商户资料
    :param merchant_identity: 商户ID
    :return: (30200, merchant)/成功，(>30200, "errmsg")/失败
    """
    try:
        # TODO... 目前无法实现按照商户ID路由到数据库
        collection = get_mongo_collection(merchant_identity, "merchant")
        if not collection:
            g_log.error("get collection merchant failed")
            return 30208, "get collection merchant failed"
        merchant = collection.find_one({"_id": ObjectId(merchant_identity)})
        if not merchant or merchant["deleted"] == 1:
            g_log.warning("merchant %s not exist", merchant_identity)
            return 30209, "merchant not exist"
        g_log.debug("get %s %s", merchant_identity, merchant)
        return 30200, merchant
    except Exception as e:
        g_log.error("%s %s", e.__class__, e)
        return 30210, "exception"

# pragma 删除商户资料API
def merchant_delete_with_numbers(numbers):
    """
    删除商户资料
    :param numbers: 商户电话号码
    :return: (30500, "yes")/成功，(>30500, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_merchant(numbers):
            g_log.warning("invalid customer account %s", numbers)
            return 30501, "invalid phone number"

        # 连接mongo，检查该商户是否已经创建
        connection = get_mongo_connection(numbers)
        if not connection:
            g_log.error("connect to mongo failed")
            return 30502, "connect to mongo failed"

        # 检查账号是否存在
        key = "user:%s" % numbers
        if not connection.exists(key):
            g_log.warning("merchant %s not exist", key)
            return 30503, "merchant not exist"
        value = connection.hget(key, "deleted")
        g_log.debug("get %s#deleted %s", key, value)
        if value == "1":
            g_log.warning("merchant %s deleted already", key)
            return 30504, "merchant not exist"

        # 删除merchant
        connection.hset(key, "deleted", 1)
        return 30500, "yes"
    except (mongo.ConnectionError, mongo.TimeoutError) as e:
        g_log.error("connect to mongo failed")
        return 30505, "connect to mongo failed"
    except Exception as e:
        g_log.error("%s", e)
        return 30506, "exception"


def merchant_delete_with_identity(merchant_identity):
    """
    删除商户资料
    :param merchant_identity: 商户ID
    :return:
    """
    try:
        # 根据商户id查找商户电话号码
        numbers = ""
        return merchant_delete_with_numbers(numbers)
    except Exception as e:
        g_log.error("%s", e)
        return 30507, "exception"


def merchant_delete(numbers=None, merchant_identity=None):
    """
    删除商户资料，商户电话号码优先
    :param numbers: 商户电话号码
    :param merchant_identity: 商户ID
    :return:
    """
    try:
        if numbers:
            return merchant_delete_with_numbers(numbers)
        elif merchant_identity:
            return merchant_delete_with_identity(merchant_identity)
        else:
            return 30508, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 30509, "exception"


def generate_merchant_identity(numbers):
    """
    随即生成商家ID ＝ random([a-z,A-Z,0-9] , 3) + random(手机号码后8位，2)，异常使用手机号码后5位
    :param numbers: 手机号码
    :return: 5位随机字符串
    """
    try:
        merchant_identity = "".join(random.sample(string.ascii_letters + string.digits, 3)
                                    + random.sample(numbers[-8:], 2))
        return merchant_identity
    except Exception as e:
        g_log.critical("%s", e)
        return numbers[-5:]

