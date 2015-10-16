# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import redis
import common_pb2
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
import package
from redis_connection import get_redis_connection
from account_valid import user_is_valid_merchant


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
        self.phone_number = self.head.phone_number
        self.code = 1   # 模块号(2位) + 功能号(2位) + 错误号(2位)
        self.message = ""

    def enter(self):
        """
        处理具体业务
        :return: 0/不回包给前端，pb/正确返回，timeout/超时
        """
        # TODO... 验证登录态
        try:
            command_handle = {100: self.merchant_create, 101: self.merchant_retrieve, 102: self.merchant_batch_retrieve,
                              103: self.merchant_update, 104: self.merchant_delete}
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
            phone_number = body.phone_number
            material = body.material

            if not phone_number:
                if not material.phone_number:
                    # TODO... 根据包体中的merchant_identity获取phone_number
                    pass
                else:
                    phone_number = material.phone_number

            # 发起请求的商户和要创建的商户不同，认为没有权限，TODO...更精细控制
            if self.phone_number != phone_number:
                g_log.warning("%s no privilege to create merchant %s", self.phone_number, phone_number)
                self.code = 30105
                self.message = "no privilege to create merchant"
                return 1

            kwargs = {"phone_number": phone_number, "merchant_name": material.merchat_name,
                      "introduce": material.introduce, "logo": material.logo, "email": material.email,
                      "country": material.country, "location": material.location,
                      "latitude": material.latitude, "longitude": material.longitude}
            g_log.debug("create merchant: %s", kwargs)
            self.code, self.message = merchant_create(kwargs)

            if 30100 == self.code:
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
            body = self.request.merchant_delete_request
            phone_number = body.phone_number
            merchant_identity = body.merchant_identity

            if not phone_number:
                # TODO... 根据包体中的merchant_identity获取phone_number
                pass

            # 发起请求的商户和要获取的商户不同，认为没有权限，TODO...更精细控制
            if self.phone_number != phone_number:
                g_log.warning("%s no privilege to retrieve merchant %s", self.phone_number, phone_number)
                self.code = 30208
                self.message = "no privilege to retrieve merchant"
                return 1

            self.code, self.message = merchant_retrieve_with_phone_number(phone_number)

            if 30200 == self.code:
                # 获取成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "delete merchant done"

                value = self.message
                material = response.merchant_retrieve_response.material
                material.phone_number = phone_number
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
            phone_number = body.phone_number
            material = body.material

            if not phone_number:
                if not material.phone_number:
                    # TODO... 根据包体中的merchant_identity获取phone_number
                    pass
                else:
                    phone_number = material.phone_number

            # 发起请求的商户和要创建的商户不同，认为没有权限，TODO...更精细控制
            if self.phone_number != phone_number:
                g_log.warning("%s no privilege to update merchant %s", self.phone_number, phone_number)
                self.code = 30400
                self.message = "no privilege to update merchant"
                return 1

            kwargs = {"phone_number": phone_number, "merchant_name": material.merchat_name,
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
            phone_number = body.phone_number
            merchant_identity = body.merchant_identity

            if not phone_number:
                # TODO... 根据包体中的merchant_identity获取phone_number
                pass

            # 发起请求的商户和要创建的商户不同，认为没有权限，TODO...更精细控制
            if self.phone_number != phone_number:
                g_log.warning("%s no privilege to delete merchant %s", self.phone_number, phone_number)
                self.code = 30510
                self.message = "no privilege to delete merchant"
                return 1

            self.code, self.message = merchant_delete_with_phone_number(phone_number)

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
    :param kwargs: {"phone_number": "18688982240", "merchant_name": "Star Bucks", "introduce": "We sell coffee",
                    "logo": "", "email": "vip@starbucks.com", "country": "USA", "location": "california",
                    "latitude": 38.38, "longitude": -114.8}
    :return: (30100, "yes")/成功，(>30100, "errmsg")/失败
    """
    try:
        # 检查要创建的商户phone_number
        phone_number = kwargs.get("phone_number", "")
        if not user_is_valid_merchant(phone_number):
            g_log.warning("invalid customer account %s", phone_number)
            return 30101, "invalid phone number"

        # 连接redis，检查该商户是否已经创建
        connection = get_redis_connection(phone_number)
        if not connection:
            g_log.error("connect to redis failed")
            return 30102, "connect to redis failed"

        key = "user:%s" % phone_number
        if connection.exists(key) and connection.hget(key, "deleted") == "0":
            g_log.warning("duplicate create user %s", key)
            return 30103, "duplicate create user"

        # 昵称不能超过16字节，超过要截取前16字节
        merchant_name = kwargs.get("merchant_name", phone_number)
        if len(merchant_name) > 32:
            g_log.warning("too long merchant_name %s", merchant_name)
            merchant_name = merchant_name[0:16]

        # 个人介绍不能超过512字节，超过要截取前512字节
        introduce = kwargs.get("introduce", "")
        if len(introduce) > 512:
            g_log.warning("too long introduce %s", introduce)
            introduce = introduce[0:512]

        # TODO... email、logo、国家、地区检查
        logo = kwargs.get("logo", "")
        email = kwargs.get("email", "")
        country = kwargs.get("country", "")
        location = kwargs.get("location", "")

        latitude = kwargs.get("latitude", 0)
        if latitude < -90 or latitude > 90:
            g_log.warning("latitude illegal, %s", latitude)
            latitude = 0
        longitude = kwargs.get("longitude", 0)
        if longitude < -180 or longitude > 180:
            g_log.warning("longitude illegal, %s", longitude)
            longitude = 0

        value = {"name": merchant_name, "logo": logo, "email": email, "introduce": introduce, "latitude": latitude,
                 "longitude": longitude, "country": country, "location": location, "deleted": 0}

        # 存入数据库
        connection.hmset(key, value)
        g_log.debug("insert %s %s", key, value)

        return 30100, "yes"
    except (redis.ConnectionError, redis.TimeoutError) as e:
        g_log.error("connect to redis failed")
        return 30102, "connect to redis failed"
    except Exception as e:
        g_log.error("%s", e)
        return 30104, "exception"


# pragma 读取商户资料API
def merchant_retrieve_with_phone_number(phone_number):
    """
    读取商户资料
    :param phone_number: 商户电话号码
    :return: (30200, "yes")/成功，(>30200, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_merchant(phone_number):
            g_log.warning("invalid customer account %s", phone_number)
            return 30201, "invalid phone number"

        # 连接redis，检查该商户是否已经创建
        connection = get_redis_connection(phone_number)
        if not connection:
            g_log.error("connect to redis failed")
            return 30202, "connect to redis failed"

        key = "user:%s" % phone_number
        if not connection.exists(key) or connection.hget(key, "deleted") == 1:
            g_log.warning("merchant %s not exist", key)
            return 30203, "merchant not exist"

        value = connection.hgetall(key)
        g_log.debug("get %s %s", key, value)
        return 30200, value
    except (redis.ConnectionError, redis.TimeoutError) as e:
        g_log.error("connect to redis failed")
        return 30202, "connect to redis failed"
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
        phone_number = ""
        return merchant_retrieve_with_phone_number(phone_number)
    except Exception as e:
        g_log.error("%s", e)
        return 30205, "exception"


def merchant_retrieve(phone_number=None, merchant_identity=None):
    """
    获取商户资料，商户电话号码优先
    :param phone_number: 商户电话号码
    :param merchant_identity: 商户ID
    :return:
    """
    try:
        if phone_number:
            return merchant_retrieve_with_phone_number(phone_number)
        elif merchant_identity:
            return merchant_retrieve_with_identity(merchant_identity)
        else:
            return 30206, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 30207, "exception"

# pragma 删除商户资料API
def merchant_delete_with_phone_number(phone_number):
    """
    删除商户资料
    :param phone_number: 商户电话号码
    :return: (30500, "yes")/成功，(>30500, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_merchant(phone_number):
            g_log.warning("invalid customer account %s", phone_number)
            return 30501, "invalid phone number"

        # 连接redis，检查该商户是否已经创建
        connection = get_redis_connection(phone_number)
        if not connection:
            g_log.error("connect to redis failed")
            return 30502, "connect to redis failed"

        # 检查账号是否存在
        key = "user:%s" % phone_number
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
    except (redis.ConnectionError, redis.TimeoutError) as e:
        g_log.error("connect to redis failed")
        return 30505, "connect to redis failed"
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
        phone_number = ""
        return merchant_delete_with_phone_number(phone_number)
    except Exception as e:
        g_log.error("%s", e)
        return 30507, "exception"


def merchant_delete(phone_number=None, merchant_identity=None):
    """
    删除商户资料，商户电话号码优先
    :param phone_number: 商户电话号码
    :param merchant_identity: 商户ID
    :return:
    """
    try:
        if phone_number:
            return merchant_delete_with_phone_number(phone_number)
        elif merchant_identity:
            return merchant_delete_with_identity(merchant_identity)
        else:
            return 30508, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 30509, "exception"
