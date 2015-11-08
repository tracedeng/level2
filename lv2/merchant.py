# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import string
import random
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.collection import ReturnDocument
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
            command_handle = {201: self.merchant_create, 202: self.merchant_retrieve, 203: self.merchant_batch_retrieve,
                              204: self.merchant_update, 205: self.merchant_delete, 206: self.merchant_create_manager,
                              207: self.merchant_create_manager, 208: self.merchant_delegate_manager,
                              209: self.merchant_delete_manager}
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

            # 发起请求的商家和要创建的商家不同，认为没有权限，TODO...更精细控制
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
            merchant_identity = body.merchant_identity

            if not numbers:
                # TODO... 根据包体中的merchant_identity获取numbers
                pass

            # 发起请求的商家和要获取的商家不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to retrieve merchant %s", self.numbers, numbers)
                self.code = 30216
                self.message = "no privilege to retrieve merchant"
                return 1
            if merchant_identity:
                self.code, self.message = merchant_retrieve_with_merchant_identity(numbers, merchant_identity)
            else:
                self.code, self.message = merchant_retrieve_with_numbers(numbers)

            if 30200 == self.code:
                # 获取成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "retrieve merchant done"

                merchants = self.message
                materials = response.merchant_retrieve_response.materials
                # g_log.debug(dir(materials))
                for value in merchants:
                    material = materials.add()
                    material.name = value["name"]
                    material.name_en = value["name_en"]
                    material.numbers = value["numbers"]
                    material.verified = char_2_yes_no(value["verified"])
                    # g_log.debug(value["_id"])
                    # g_log.debug(value["_id"].__class__)
                    material.identity = str(value["_id"])
                    material.logo = value["logo"]
                    material.email = value["email"]
                    material.qrcode = value["qrcode"]
                    material.introduce = value["introduce"]
                    material.contact_numbers = value["contact_numbers"]
                    material.contract = value["contract"]
                    material.location = value["location"]
                    material.country = value["country"]
                    material.latitude = float(value["latitude"])
                    material.longitude = float(value["longitude"])
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s %s", e.__class__, e)
            return 0

    def merchant_batch_retrieve(self):
        pass

    def merchant_update(self):
        """
        修改merchant资料
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的merchant
        4 merchant写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.merchant_update_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity
            material = body.material

            if not numbers:
                if not material.numbers:
                    # TODO... 根据包体中的identity获取numbers
                    pass
                else:
                    numbers = material.numbers

            # 发起请求的商家和要创建的商家不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to update merchant %s", self.numbers, numbers)
                self.code = 30411
                self.message = "no privilege to update merchant"
                return 1

            value = {}
            # TODO... HasField 问题
            # g_log.debug(dir(material))
            # g_log.debug(body.HasField('material'))
            # g_log.debug(material.HasField(material.nickname))
            # g_log.debug(material.ListFields()[0][0].name)
            # g_log.debug("create merchant: %s", kwargs)
            if material.HasField('name'):
                value["name"] = material.name

            if material.HasField("name_en"):
                value["name_en"] = material.name_en

            if material.HasField("logo"):
                value["logo"] = material.logo

            if material.HasField("email"):
                value["email"] = material.email

            if material.HasField("introduce"):
                value["introduce"] = material.introduce

            if material.HasField("country"):
                value["country"] = material.country

            if material.HasField("location"):
                value["location"] = material.location

            if material.HasField("contact_numbers"):
                value["contact_numbers"] = material.contact_numbers

            if material.HasField("qrcode"):
                value["qrcode"] = material.qrcode
            
            if material.HasField("contract"):
                value["contract"] = material.contract
            
            if material.HasField("latitude"):
                value["latitude"] = material.latitude
                
            if material.HasField("longitude"):
                value["longitude"] = material.longitude

            # 认证标志修改参考merchant_update_verified
            # if material.HasField("verified"):
            #     value["verified"] = material.verified
                
            g_log.debug("update merchant material: %s", value)
            self.code, self.message = merchant_update_with_numbers(numbers, merchant_identity, **value)

            if 30400 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "update merchant material done"
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0 

    def merchant_update_verified(self):
        """
        修改商家认证标志
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的merchant
        4 merchant写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.merchant_update_verified_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity
            material = body.material

            if not numbers:
                if not material.numbers:
                    # TODO... 根据包体中的identity获取numbers
                    pass
                else:
                    numbers = material.numbers

            # 发起请求的商家和要创建的商家不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to update merchant %s", self.numbers, numbers)
                self.code = 30511
                self.message = "no privilege to update merchant"
                return 1

            # TODO... HasField 问题
            if not material.HasField("verified"):
                g_log.error("lost argument verified")
                self.code = 30512
                self.message = "illegal argument"
                return 1

            verified = material.verified
            self.code, self.message = merchant_update_verified_with_numbers(numbers, merchant_identity, verified)

            if 30500 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "update field verified done"

                response.merchant_update_verified_response.verified = self.message
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def merchant_delete(self):
        """
        单独删除商家的操作几乎不出现，先保留
        :return:
        """
        pass

    def merchant_create_manager(self):
        """
        创建merchant资料
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的merchant
        4 merchant写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.merchant_create_manager_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity
            manager_numbers = body.manager_numbers

            if not numbers:
                # TODO... 根据包体中的identity获取numbers
                identity = body.identity

            # 发起请求的商家和商家创建人不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to create merchant manager %s", self.numbers, numbers)
                self.code = 30709
                self.message = "no privilege to create merchant manager"
                return 1

            kwargs = {"manager_numbers": manager_numbers, "merchant_identity": merchant_identity,
                      "merchant_founder": numbers}
            g_log.debug("create numbers merchant: %s", kwargs)
            self.code, self.message = merchant_create_manager(**kwargs)

            if 30700 == self.code:
                # 创建成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "create numbers merchant done"

                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def merchant_delegate_manager(self):
        """
        创建人将商家委托给其它管理员
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的merchant
        4 merchant写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.merchant_delegate_manager_request
            numbers = body.numbers
            merchant_identity = body.merchant_identity
            delegate_numbers = body.delegate_numbers

            if not numbers:
                # TODO... 根据包体中的identity获取numbers
                identity = body.identity

            if not delegate_numbers:
                # TODO... 根据包体中的delegate_identity获取number
                delegate_identity = body.delegate_identity

            # 发起请求的商家和商家创建人不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to create merchant manager %s", self.numbers, numbers)
                self.code = 30813
                self.message = "no privilege to create merchant manager"
                return 1

            kwargs = {"delegate_numbers": delegate_numbers, "merchant_identity": merchant_identity,
                      "merchant_founder": numbers}
            g_log.debug("founder %s delegate merchant %s to manager: %s", numbers, merchant_identity, delegate_numbers)
            self.code, self.message = merchant_delegate_manager(**kwargs)

            if 30800 == self.code:
                # 创建成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "delegate merchant to manager done"

                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def merchant_delete_manager(self):
        """
        删除merchant资料的管理员
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的merchant
        4 merchant写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.merchant_delete_request
            numbers = body.numbers
            identity = body.identity
            merchant_identity = body.merchant_identity

            if not numbers:
                # TODO... 根据包体中的merchant_identity获取numbers
                pass

            # 发起请求的商家和要创建的商家不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to delete merchant %s", self.numbers, numbers)
                self.code = 30914
                self.message = "no privilege to delete merchant"
                return 1

            if merchant_identity:
                self.code, self.message = merchant_delete_with_merchant_identity(numbers, merchant_identity)
            else:
                self.code, self.message = merchant_delete_with_numbers(numbers)

            if 30900 == self.code:
                # 删除成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "delete merchant manager done"
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


# ---------------------------APIAPIAPIAPIAPIAPIAPIAPIAPIAPIAPIAPAPIAPIAPIAPIAPIAPAPIAPIAPIAPI
# pragma 增加商家资料API
def merchant_create(**kwargs):
    """
    增加商家资料
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

        # 商家名称不能超过64字节，超过要截取前64字节
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

        # 商家介绍不能超过512字节，超过要截取前512字节
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
                 "contact_numbers": contact_numbers, "deleted": 0, "numbers": numbers, "create_time": datetime.now()}
        
        # 创建商家资料，商家和资料关联，TODO... 事务
        collection = get_mongo_collection(numbers, "merchant")
        if not collection:
            g_log.error("get collection merchant failed")
            return 30103, "get collection merchant failed"
        merchant_identity = collection.insert_one(value).inserted_id
        merchant_identity = str(merchant_identity)
        g_log.debug("insert merchant %s", value)

        collection = get_mongo_collection(numbers, "numbers_merchant")
        if not collection:
            g_log.error("get collection numbers_merchant failed")
            return 30104, "get collection numbers_merchant failed"

        collection.insert_one({"numbers": numbers, "merchant_founder": numbers,
                               "merchant_identity": merchant_identity, "deleted": 0})
        g_log.debug("insert merchant numbers many-many relation, %s:%s", merchant_identity, numbers)
        return 30100, merchant_identity
    # except (mongo.ConnectionError, mongo.TimeoutError) as e:
    #     g_log.error("connect to mongo failed")
    #     return 30102, "connect to mongo failed"
    except Exception as e:
        g_log.error("%s %s", e.__class__, e)
        return 30105, "exception"


# pragma 读取商家资料API
def merchant_retrieve_with_numbers(numbers):
    """
    读取管理员所有商家资料
    :param numbers: 商家电话号码
    :return: (30200, merchants)/成功，(>30200, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_merchant(numbers):
            g_log.warning("invalid merchant account %s", numbers)
            return 30201, "invalid phone number"

        # 获取商家拥有的所有商家ID
        collection = get_mongo_collection(numbers, "numbers_merchant")
        if not collection:
            g_log.error("get collection numbers_merchant failed")
            return 30202, "get collection numbers_merchant failed"
        numbers_merchants = collection.find({"numbers": numbers, "deleted": 0},
                                    {"merchant_identity": 1, "merchant_founder": 1, "_id": 0})
        g_log.debug("[numbers_merchant] numbers %s managers %s merchant", numbers, numbers_merchants.count())
        merchants = []
        for numbers_merchant in numbers_merchants:
            merchant_founder = numbers_merchant["merchant_founder"]
            merchant_identity = numbers_merchant["merchant_identity"]
            g_log.debug("merchant %s, founder %s", merchant_identity, merchant_founder)
            # identities.append(ObjectId(merchant["merchant_identity"]))

            collection = get_mongo_collection(merchant_founder, "merchant")
            if not collection:
                g_log.error("get collection merchant %s failed", merchant_founder)
                return 30203, "get collection merchant failed"
            merchant = collection.find_one({"_id": ObjectId(merchant_identity), "deleted": 0})
            if merchant:
                merchants.append(merchant)
            else:
                g_log.warn("merchant %s not exist", merchant_identity)
        g_log.debug("[merchant] numbers %s managers %s merchant", numbers, len(merchants))

        # TODO...对没有商家做特殊处理
        if not len(merchants):
            g_log.debug("account %s managers no merchant", numbers)

        return 30200, merchants
    except Exception as e:
        g_log.error("%s", e)
        return 30204, "exception"


def merchant_retrieve_with_identity(identity):
    """
    读取管理员所有商家资料
    :param identity: 商家管理员ID
    :return:
    """
    try:
        # 根据商家id查找商家电话号码
        numbers = ""
        return merchant_retrieve_with_numbers(numbers)
    except Exception as e:
        g_log.error("%s", e)
        return 30205, "exception"


def merchant_retrieve(numbers=None, identity=None):
    """
    读取管理员所有商家资料，商家电话号码优先
    :param numbers: 商家管理员电话号码
    :param identity: 商家管理员ID
    :return:
    """
    try:
        if numbers:
            return merchant_retrieve_with_numbers(numbers)
        elif identity:
            return merchant_retrieve_with_identity(identity)
        else:
            return 30206, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 30207, "exception"


def merchant_retrieve_with_merchant_identity(numbers, merchant_identity):
    """
    获取指定商家管理员ID的商家资料
    :param numbers: 商家管理员
    :param merchant_identity: 商家ID
    :return: (30200, [merchant])/成功，(>30200, "errmsg")/失败
    """
    try:
        # 找到商家创建人numbers
        collection = get_mongo_collection(numbers, "numbers_merchant")
        if not collection:
            g_log.error("get collection number merchant failed")
            return 30208, "get collection number merchant failed"
        merchant = collection.find_one({"numbers": numbers, "merchant_identity": merchant_identity, "deleted": 0},
                                       {"merchant_founder": 1})
        if not merchant:
            g_log.warn("merchant %s not exist", merchant_identity)
            return 30209, "merchant not exist"

        merchant_founder = merchant["merchant_founder"]
        g_log.debug("merchant founder %s", merchant_founder)
        collection = get_mongo_collection(merchant_founder, "merchant")
        if not collection:
            g_log.error("get collection merchant failed")
            return 30210, "get collection merchant failed"
        merchant = collection.find_one({"_id": ObjectId(merchant_identity), "deleted": 0})
        if not merchant:
            g_log.warning("merchant %s not exist", merchant_identity)
            return 30211, "merchant not exist"
        g_log.debug("get %s %s", merchant_identity, merchant)
        return 30200, [merchant]    # 返回的要是一个list和merchant_retrieve_with_numbers一致
    except Exception as e:
        g_log.error("%s %s", e.__class__, e)
        return 30212, "exception"


def merchant_retrieve_with_merchant_identity_only(merchant_identity):
    """
    获取指定商家ID的商家资料
    :param merchant_identity: 商家ID
    :return: (30200, [merchant])/成功，(>30200, "errmsg")/失败
    """
    try:
        # TODO... 需要广播，待数据层独立后处理，当前认为只有一个数据库
        collection = get_mongo_collection("", "merchant")
        if not collection:
            g_log.error("get collection merchant failed")
            return 30213, "get collection merchant failed"
        merchant = collection.find_one({"_id": ObjectId(merchant_identity), "deleted": 0})
        if not merchant:
            g_log.warning("merchant %s not exist", merchant_identity)
            return 30214, "merchant not exist"
        g_log.debug("get %s %s", merchant_identity, merchant)
        return 30200, [merchant]    # 返回的要是一个list和merchant_retrieve_with_numbers一致
    except Exception as e:
        g_log.error("%s %s", e.__class__, e)
        return 30215, "exception"


# pragma 更新商家资料API
def merchant_update_with_numbers(numbers, merchant_identity, **kwargs):
    """
    更新商家资料
    :param numbers: 商家管理员迪纳好号码
    :param merchant_identity: 商家Id
    :param kwargs:
    :return: (30400, "yes")/成功，(>30400, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_merchant(numbers):
            g_log.warning("invalid manager number %s", numbers)
            return 30401, "invalid phone number"

        # 检查该商家是否存在
        collection = get_mongo_collection(numbers, "numbers_merchant")
        if not collection:
            g_log.error("get collection number merchant failed")
            return 30402, "get collection number merchant failed"
        merchant = collection.find_one({"numbers": numbers, "merchant_identity": merchant_identity, "deleted": 0},
                                       {"merchant_founder": 1})
        if not merchant:
            g_log.warn("merchant %s not exist", merchant_identity)
            return 30403, "merchant not exist"
        merchant_founder = merchant["merchant_founder"]
        g_log.debug("merchant founder %s", merchant_founder)

        value = {}
        # 名称不能超过64字节，超过要截取前64字节
        name = kwargs.get("name")
        if name and len(name) > 64:
            g_log.warning("too long name %s", name)
            name = name[0:64]
            value["name"] = name

        name_en = kwargs.get("name_en")
        if name_en and len(name_en) > 64:
            g_log.warning("too long name_en %s", name_en)
            name_en = name_en[0:64]
            value["name_en"] = name_en

        # 商家介绍不能超过512字节，超过要截取前512字节
        introduce = kwargs.get("introduce")
        if introduce and len(introduce) > 512:
            g_log.warning("too long introduce %s", introduce)
            introduce = introduce[0:512]
            value["introduce"] = introduce

        # TODO... 头像、email、logo、国家、地区检查
        logo = kwargs.get("logo")
        if logo:
            value["logo"] = logo
        email = kwargs.get("email")
        if email:
            value["email"] = email
        country = kwargs.get("country")
        if country:
            value["country"] = country
        location = kwargs.get("location")
        if location:
            value["location"] = location
        qrcode = kwargs.get("qrcode")
        if qrcode:
            value["qrcode"] = qrcode
        contract = kwargs.get("contract")
        if contract:
            value["contract"] = contract
        contact_numbers = kwargs.get("contact_numbers")
        if contact_numbers:
            value["contact_numbers"] = contact_numbers

        latitude = kwargs.get("latitude")
        if latitude:
            if latitude < -90 or latitude > 90:
                g_log.warning("latitude illegal, %s", latitude)
                latitude = 0
            value["latitude"] = latitude
        longitude = kwargs.get("longitude", 0)
        if longitude:
            if longitude < -180 or longitude > 180:
                g_log.warning("longitude illegal, %s", longitude)
                longitude = 0
            value["longitude"] = longitude

        # 存入数据库
        collection = get_mongo_collection(merchant_founder, "merchant")
        if not collection:
            g_log.error("get collection merchant failed")
            return 30404, "get collection merchant failed"
        g_log.debug("update merchant %s: %s", merchant_identity, value)
        if name or name_en:
            # 未认证才可修改商家名称
            merchant = collection.find_one_and_update({"_id": ObjectId(merchant_identity),
                                                       "verified": yes_no_2_char("no"), "deleted": 0},
                                                      {"$set", value},
                                                      return_document=ReturnDocument.AFTER)
            if merchant and merchant.verified == "y":
                g_log.error("update name of verified merchant is forbidden")
                return 30405, "update name of verified merchant is forbidden"
        else:
            merchant = collection.find_one_and_update({"_id": ObjectId(merchant_identity), "deleted": 0},
                                                      {"$set", value})
        if not merchant or merchant.modified_count != 1:
            g_log.warning("match count:%s, modified count:%s", merchant.match_count, merchant.modified_count)
            return 30406, "update failed"
        return 30400, "yes"
    # except (redis.ConnectionError, redis.TimeoutError) as e:
    #     g_log.error("connect to redis failed")
    #     return 20405, "connect to redis failed"
    except Exception as e:
        g_log.error("%s", e)
        return 30407, "exception"


def merchant_update_with_identity(identity, merchant_identity, **kwargs):
    """
    更商家资料
    :param identity: 商家管理员ID
    :param merchant_identity: 商家Id
    :param kwargs:
    :return:
    """
    try:
        # 根据商家id查找商家电话号码
        numbers = ""
        return merchant_update_with_numbers(numbers, **kwargs)
    except Exception as e:
        g_log.error("%s", e)
        return 30408, "exception"


def merchant_update(merchant_identity, numbers=None, identity=None, **kwargs):
    """
    更新商家资料，商家电话号码优先
    :param merchant_identity: 商家ID
    :param numbers: 管理员电话号码
    :param identity: 管理员ID
    :param kwargs:
    :return:
    """
    try:
        if numbers:
            return merchant_update_with_numbers(numbers, merchant_identity, **kwargs)
        elif identity:
            return merchant_update_with_identity(identity, merchant_identity, **kwargs)
        else:
            return 30409, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 30410, "exception"


# pragma 更新商家资料API
def merchant_update_verified_with_numbers(numbers, merchant_identity, verified):
    """
    更新商家资料验证标志位
    :param numbers: 管理员电话号码码
    :param merchant_identity: 商家ID
    :param verified: 验证标志, yes|no
    :return: (30500, 验证标志yes|no)/成功，(>30500, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_merchant(numbers):
            g_log.warning("invalid customer account %s", numbers)
            return 30501, "invalid phone number"

        # 检查该商家是否存在
        collection = get_mongo_collection(numbers, "numbers_merchant")
        if not collection:
            g_log.error("get collection number merchant failed")
            return 30502, "get collection number merchant failed"
        merchant = collection.find_one({"numbers": numbers, "merchant_identity": merchant_identity, "deleted": 0},
                                       {"merchant_founder": 1})
        if not merchant:
            g_log.warn("merchant %s not exist", merchant_identity)
            return 30503, "merchant not exist"
        merchant_founder = merchant["merchant_founder"]
        g_log.debug("merchant founder %s", merchant_founder)

        # 存入数据库
        collection = get_mongo_collection(merchant_founder, "merchant")
        if not collection:
            g_log.error("get collection merchant failed")
            return 30504, "get collection merchant failed"
        merchant = collection.find_one_and_update({"_id": ObjectId(merchant_identity), "deleted": 0},
                                                  {"$set": {"verified": yes_no_2_char(verified)}},
                                                  return_document=ReturnDocument.AFTER)
        if not merchant:
            g_log.warning("merchant %s not exist", merchant_identity)
            return 30506, "merchant not exist"
        return 30500, char_2_yes_no(merchant.verified)
    # except (redis.ConnectionError, redis.TimeoutError) as e:
    #     g_log.error("connect to redis failed")
    #     return 20405, "connect to redis failed"
    except Exception as e:
        g_log.error("%s", e)
        return 30507, "exception"


def merchant_update_verified_with_identity(identity, merchant_identity, verified):
    """
    更商家资料
    :param identity: 商家管理员ID
    :param merchant_identity: 商家ID
    :param verified: 验证标志, yes|no
    :return:
    """
    try:
        # 根据商家id查找商家电话号码
        numbers = ""
        return merchant_update_verified_with_numbers(numbers, merchant_identity, verified)
    except Exception as e:
        g_log.error("%s", e)
        return 30508, "exception"


def merchant_update_verified(merchant_identity, verified, numbers=None, identity=None):
    """
    更新商家资料，商家电话号码优先
    :param merchant_identity: 商家ID
    :param verified: 验证标志, yes|no
    :param numbers: 管理员电话号码
    :param identity: 管理员ID
    :return:
    """
    try:
        if numbers:
            return merchant_update_verified_with_numbers(numbers, merchant_identity, verified)
        elif identity:
            return merchant_update_verified_with_identity(identity, merchant_identity, verified)
        else:
            return 30509, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 30510, "exception"


def merchant_create_manager(**kwargs):
    """
    增加商家管理员
    :param kwargs: {"manager_numbers": "18688982241", "merchant_identity": "562726ad4e79150235f20b64",
                    "merchant_founder": "18688982240"}
    :return: (30600, yes)/成功，(>30600, "errmsg")/失败
    """
    try:
        # 检查管理员numbers
        manager_numbers = kwargs.get("manager_numbers", "")
        if not user_is_valid_merchant(manager_numbers):
            g_log.error("invalid merchant account %s", manager_numbers)
            return 30701, "invalid merchant account"

        # 检查创建者numbers
        merchant_founder = kwargs.get("merchant_founder", "")
        if not user_is_valid_merchant(merchant_founder):
            g_log.error("invalid merchant account %s", merchant_founder)
            return 30702, "invalid merchant account"

        # 商家ID检查
        merchant_identity = kwargs.get("merchant_identity")
        if not merchant_identity or len(merchant_identity) != 24:
            g_log.error("invalid merchant identity")
            return 30703, "invalid merchant identity"

        # 检查商家是否存在
        value = {"numbers": merchant_founder, "merchant_founder": merchant_founder,
                 "merchant_identity": merchant_identity, "deleted": 0}
        collection = get_mongo_collection(merchant_founder, "numbers_merchant")
        if not collection:
            g_log.error("get collection numbers_merchant failed")
            return 30704, "get collection numbers_merchant failed"
        result = collection.find_one(value)

        if not result:
            g_log.error("merchant %s not exist", merchant_identity)
            return 30705, "merchant_identity not exist"

        # 新增管理员
        value = {"numbers": manager_numbers, "merchant_founder": merchant_founder,
                 "merchant_identity": merchant_identity, "deleted": 0}
        collection = get_mongo_collection(manager_numbers, "numbers_merchant")
        if not collection:
            g_log.error("get collection numbers_merchant failed")
            return 30706, "get collection numbers_merchant failed"
        # collection.insert_one(value)
        relation = collection.find_one_and_update({"numbers": manager_numbers, "merchant_identity": merchant_identity},
                                                  {'$set': value}, upsert=True)
        if relation:
            g_log.warn("numbers %s already manager merchant %s", manager_numbers, merchant_identity)
            return 30707, "already manager"

        g_log.debug("insert merchant numbers many-many relation, %s:%s", merchant_identity, manager_numbers)
        return 30700, "yes"
    except Exception as e:
        g_log.error("%s %s", e.__class__, e)
        return 30708, "exception"


def merchant_delegate_manager(**kwargs):
    """
    创建人将商家委托给其它管理员
    1 检查是创建人创建的商家
    2 检查被委托管理员是该商家的管理员
    3 将商家资料创建人修改为新创建人，商家ID不做修改
    4 将与该商家关联的管理员的创建人field修改成新创建人
    :param kwargs: {"delegate_numbers": "18688982241", "merchant_identity": "562726ad4e79150235f20b64",
                    "merchant_founder": "18688982240"}
    :return: (30800, yes)/成功，(>30800, "errmsg")/失败
    """
    try:
        # 检查管理员numbers
        delegate_numbers = kwargs.get("delegate_numbers", "")
        if not user_is_valid_merchant(delegate_numbers):
            g_log.error("invalid merchant account %s", delegate_numbers)
            return 30801, "invalid merchant account"

        # 检查创建者numbers
        merchant_founder = kwargs.get("merchant_founder", "")
        if not user_is_valid_merchant(merchant_founder):
            g_log.error("invalid merchant account %s", merchant_founder)
            return 30802, "invalid merchant account"

        # 商家ID检查
        merchant_identity = kwargs.get("merchant_identity")
        if not merchant_identity or len(merchant_identity) != 24:
            g_log.error("invalid merchant identity")
            return 30803, "invalid merchant identity"

        # 检查是创建人创建的商家
        value = {"numbers": merchant_founder, "merchant_founder": merchant_founder,
                 "merchant_identity": merchant_identity, "deleted": 0}
        collection = get_mongo_collection(merchant_founder, "numbers_merchant")
        if not collection:
            g_log.error("get collection numbers_merchant failed")
            return 30804, "get collection numbers_merchant failed"
        result = collection.find_one(value)
        if not result:
            g_log.error("manager %s not merchant %s founder", merchant_founder, merchant_identity)
            return 30805, "manager is not founder"

        # 检查被委托管理员是该商家的管理员
        value = {"numbers": delegate_numbers, "merchant_founder": merchant_founder,
                 "merchant_identity": merchant_identity, "deleted": 0}
        collection = get_mongo_collection(delegate_numbers, "numbers_merchant")
        if not collection:
            g_log.error("get collection numbers_merchant failed")
            return 30806, "get collection numbers_merchant failed"
        result = collection.find_one(value)
        if not result:
            g_log.error("delegate manager %s is not merchant %s manager", delegate_numbers, merchant_identity)
            return 30807, "delegate manager is not merchant manager"

        # TODO... 事务
        # 读取商家资料
        collection = get_mongo_collection(merchant_founder, "merchant")
        if not collection:
            g_log.error("get collection merchant failed")
            return 30808, "get collection merchant failed"
        merchant = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
                                                {"$set": {"deleted": 1}})
        if not merchant or merchant.modified_count != 1:
            g_log.error("merchant %s not exist", merchant_identity)
            return 30809, "merchant not exist"

        # 修改商家资料为新的创建人，插入数据库
        collection_delegate = get_mongo_collection(delegate_numbers, "merchant")
        if not collection_delegate:
            g_log.error("get collection merchant failed")
            return 30810, "get collection merchant failed"
        merchant.numbers = delegate_numbers
        result = collection_delegate.insert_one(merchant)
        if not result:
            g_log.error("insert merchant failed")
            return 30811, "insert merchant failed"

        # 将与该商家关联的管理员的创建人field修改成新创建人员
        # TODO...待数据层独立时处理，目前只考虑单机，逻辑层数据层合并
        collection = get_mongo_collection(delegate_numbers, "numbers_merchant")
        if not collection:
            g_log.error("get collection numbers_merchant failed")
            return 30812, "get collection numbers_merchant failed"
        result = collection.find_one_and_update({"merchant_identity": merchant_identity},
                                                {'$set': {"merchant_founder": delegate_numbers}})
        if result:
            g_log.debug("match count:%s, update count:%s", result.match_count, result.update_count)
        return 30800, "yes"
    except Exception as e:
        g_log.error("%s %s", e.__class__, e)
        return 30813, "exception"


# pragma 删除商家API
def merchant_delete_manager_with_numbers(numbers):
    """
    删除管理员拥有的所有商家关联
    :param numbers: 商家管理员电话号码
    :return: (30900, "yes")/成功，(>30900, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_merchant(numbers):
            g_log.warning("invalid manager %s", numbers)
            return 30901, "invalid manager"
        
        # 找到管理员所有商家，删除管理员和商家关联
        collection = get_mongo_collection(numbers, "numbers_merchant")
        if not collection:
            g_log.error("get collection number merchant failed")
            return 30902, "get collection number merchant failed"
        merchants = collection.find({"numbers": numbers, "deleted": 0},
                                    {"merchant_founder": True, "merchant_identity": True, "_id": False})
        # merchants = collection.find_one_and_update({"numbers": numbers, "deleted": 0}, {"$set": {"deleted": 1}},
        #                                            projection={"merchant_founder": True, "merchant_identity": True,
        #                                                        "_id": False}, return_document=ReturnDocument.AFTER)
        # if not merchants or merchants.modified_count == 0:
        #     g_log.warn("manager %s owns no merchant", numbers)
        #     return 30903, "not own any merchant"
        if not merchants:
            g_log.warning("manager %s owns no merchant", numbers)
        
        # 如果管理员拥有该商家则同时删除该商家资料，并删除与该商家的所有关联
        for merchant in merchants:
            merchant_founder = merchant["merchant_founder"]
            merchant_identity = merchant["merchant_identity"]
            g_log.debug("merchant founder %s", merchant_founder)

            # 要删除的管理员是当前商家的创建者，则删除该商家的所有管理员，并且删除商家资料
            yes = 0
            no = 0
            if numbers == merchant_founder:
                code, message = merchant_delete_manager_with_merchant_identity(numbers, merchant_identity)
                if code == 30900:
                    g_log.debug("delete merchant %s all manager done", merchant_identity)
                    yes += 1
                else:
                    g_log.error("delete merchant %s all manager failed", merchant_identity)
                    no += 1
        g_log.debug("delete merchant all managers success: %s, failed: %s", yes, no)
        return 30900, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 30906, "exception"


def merchant_delete_manager_with_identity(identity):
    """
    删除商家
    :param identity: 商家管理员ID
    :return:
    """
    try:
        # 根据商家id查找商家电话号码
        numbers = ""
        return merchant_delete_manager_with_numbers(numbers)
    except Exception as e:
        g_log.error("%s", e)
        return 30907, "exception"


def merchant_delete(numbers=None, identity=None):
    """
    删除商家，商家管理员电话号码优先
    :param numbers: 商家管理员电话号码
    :param identity: 商家管理员ID
    :return:
    """
    try:
        if numbers:
            return merchant_delete_manager_with_numbers(numbers)
        elif identity:
            return merchant_delete_manager_with_identity(identity)
        else:
            return 30508, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 30509, "exception"


def merchant_delete_manager_with_merchant_identity(numbers, merchant_identity):
    """
    删除商家管理员和指定商家的关联
    :param numbers: 商家管理员电话号码
    :param merchant_identity: 商家ID
    :return: (30500, "yes")/成功，(>30500, "errmsg")/失败
    """
    try:
        # 找到商家创建人numbers
        collection = get_mongo_collection(numbers, "numbers_merchant")
        if not collection:
            g_log.error("get collection numbers merchant failed")
            return 30910, "get collection numbers merchant failed"
        # merchant = collection.find_one_and_update({"numbers": numbers, "merchant_identity": merchant_identity,
        merchants = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0}, 
                                                   {"$set": {"deleted": 1}},
                                                   projection={"merchant_founder": True, "_id": False})
        if not merchants:
            g_log.warn("merchant %s not exist", merchant_identity)
            return 30911, "merchant not exist"

        merchant_founder = merchants[0]["merchant_founder"]
        g_log.debug("merchant founder %s", merchant_founder)

        # 要删除的管理员是当前商家的创建者，则删除该商家的所有管理员，并且删除商家资料
        if numbers == merchant_founder:
            # 删除商家资料
            collection = get_mongo_collection(merchant_founder, "merchant")
            if not collection:
                g_log.error("get collection merchant failed")
                return 30912, "get collection merchant failed"
            merchant = collection.find_one_and_update({"_id": ObjectId(merchant_identity), "deleted": 0},
                                                      {"$set": {"deleted": 1}})
            if not merchant or merchant.modified_count != 1:
                g_log.error("delete merchant %s failed", merchant_identity)
                return 30913, "but delete merchant failed"

            # TODO...广播删除该商家的所有管理员，没法获取merchant_identity列表，所以广播给所有分片数据库处理，
            # 待数据层独立时处理，目前只考虑单机，逻辑层数据层合
            collection = get_mongo_collection(numbers, "numbers_merchant")
            if not collection:
                g_log.error("get collection numbers merchant failed")
                return 30910, "get collection numbers merchant failed"
            result = collection.find_one_and_update({"merchant_identity": merchant_identity, "deleted": 0},
                                                    {"$set": {"deleted": 1}})
            if result:
                g_log.debug("match count:%s, update count:%s", result.match_count, result.update_count)
        return 30900, "yes"
    except Exception as e:
        g_log.error("%s %s", e.__class__, e)
        return 30914, "exception"


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


def merchant_exist(merchant_identity):
    """
    查找商家是否存在
    :param merchant_identity: 商家ID
    :return: None/不存在，商家/存在
    """
    try:
        collection = get_mongo_collection("", "merchant")
        if not collection:
            g_log.error("get collection merchant failed")
            return 31001, "get collection merchant failed"
        merchant = collection.find_one({"_id": ObjectId(merchant_identity), "deleted": 0})
        return merchant
    except Exception as e:
        g_log.critical("%s", e)
        return None


def user_is_merchant_manager(manager, merchant_identity):
    """
    是否商家管理员
    :param manager: 管理员号码
    :param merchant_identity: 商家ID
    :return: None/否，商家管理员关系/是
    """
    try:
        collection = get_mongo_collection(manager, "numbers_merchant")
        if not collection:
            g_log.error("get collection numbers merchant failed")
            return 31101, "get collection numbers merchant failed"
        merchant = collection.find_one({"merchant_identity": merchant_identity, "numbers": manager, "deleted": 0})
        return merchant
    except Exception as e:
        g_log.critical("%s", e)
        return None


def merchant_material_copy_from_document(material, value):
    """
    mongo中的单条商家记录赋值给MerchantMaterial
    :param material: MerchantMaterial
    :param value: 单个商家资料document
    :return:
    """
    material.name = value["name"]
    material.name_en = value["name_en"]
    material.numbers = value["numbers"]
    material.verified = char_2_yes_no(value["verified"])
    # g_log.debug(value["_id"])
    # g_log.debug(value["_id"].__class__)
    material.identity = str(value["_id"])
    material.logo = value["logo"]
    material.email = value["email"]
    material.qrcode = value["qrcode"]
    material.introduce = value["introduce"]
    material.contact_numbers = value["contact_numbers"]
    material.contract = value["contract"]
    material.location = value["location"]
    material.country = value["country"]
    material.latitude = float(value["latitude"])
    material.longitude = float(value["longitude"])