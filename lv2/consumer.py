# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

# import redis
# from redis_connection import get_redis_connection
from datetime import datetime
from mongo_connection import get_mongo_collection
import common_pb2
import package
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from account_valid import user_is_valid_consumer, sexy_number_2_string, sexy_string_2_number


class Consumer():
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
            command_handle = {101: self.consumer_create, 102: self.consumer_retrieve, 103: self.consumer_batch_retrieve,
                              104: self.consumer_update, 105: self.consumer_delete}
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

    def consumer_create(self):
        """
        创建consumer资料
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的consumer
        4 consumer写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.consumer_create_request
            numbers = body.numbers
            material = body.material

            if not numbers:
                if not material.numbers:
                    # TODO... 根据包体中的identity获取numbers
                    pass
                else:
                    numbers = material.numbers

            # 发起请求的用户和要创建的用户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to create consumer %s", self.numbers, numbers)
                self.code = 20105
                self.message = "no privilege to create consumer"
                return 1

            kwargs = {"numbers": numbers, "nickname": material.nickname, "avatar": material.avatar,
                      "email": material.email, "sexy": material.sexy, "age": material.age,
                      "introduce": material.introduce, "country": material.country, "location": material.location,
                      "qrcode": material.qrcode}
            g_log.debug("create consumer: %s", kwargs)
            self.code, self.message = consumer_create(**kwargs)

            if 20100 == self.code:
                # 创建成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "create consumer done"
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def consumer_retrieve(self):
        """
        获取consumer资料
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的consumer
        4 consumer写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.consumer_retrieve_request
            numbers = body.numbers
            identity = body.identity

            if not numbers:
                # TODO... 根据包体中的identity获取numbers
                pass

            # 发起请求的用户和要获取的用户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to retrieve consumer %s", self.numbers, numbers)
                self.code = 20208
                self.message = "no privilege to retrieve consumer"
                return 1

            self.code, self.message = consumer_retrieve_with_numbers(numbers)

            if 20200 == self.code:
                # 获取成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "retrieve consumer done"

                material = response.consumer_retrieve_response.material
                material.numbers = numbers
                value = self.message
                material.sexy = sexy_number_2_string(value["sexy"])  # 从redis取出来的值都是字符串
                material.age = int(value["age"])
                material.introduce = value["introduce"]
                material.email = value["email"]
                material.nickname = value["name"]
                material.location = value["location"]
                material.country = value["country"]
                material.qrcode = value["qrcode"]
                material.avatar = value["avatar"]
                material.create_time = value["create_time"].strftime("%Y-%m-%d %H:%M:%S")
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def consumer_batch_retrieve(self):
        pass

    def consumer_update(self):
        """
        修改consumer资料
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的consumer
        4 consumer写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.consumer_update_request
            numbers = body.numbers
            material = body.material

            if not numbers:
                if not material.numbers:
                    # TODO... 根据包体中的identity获取numbers
                    pass
                else:
                    numbers = material.numbers

            # 发起请求的用户和要创建的用户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to update consumer %s", self.numbers, numbers)
                self.code = 20410
                self.message = "no privilege to update consumer"
                return 1

            value = {}
            # TODO... HasField 问题
            # g_log.debug(dir(material))
            g_log.debug(body.HasField('material'))
            # g_log.debug(body.HasField('numbers'))
            # g_log.debug(material.HasField(material.nickname))
            g_log.debug(material.ListFields()[0][0].name)
            g_log.debug(material.ListFields()[1][0].name)
            g_log.debug("%s", material.ListFields())
            g_log.debug("%s", body.ListFields())
            if material.HasField('nickname'):
                value["nickname"] = material.nickname

            if material.HasField("sexy"):
                value["sexy"] = material.sexy

            if material.HasField("age"):
                value["age"] = material.age

            if material.HasField("email"):
                value["email"] = material.email

            if material.HasField("introduce"):
                value["introduce"] = material.introduce

            if material.HasField("country"):
                value["country"] = material.country

            if material.HasField("location"):
                value["location"] = material.location

            if material.HasField("avatar"):
                value["avatar"] = material.avatar

            if material.HasField("qrcode"):
                value["qrcode"] = material.qrcode
            g_log.debug("create consumer: %s", value)
            self.code, self.message = consumer_update_with_numbers(numbers, **value)

            if 20400 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "update consumer done"
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def consumer_delete(self):
        """
        删除consumer资料
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的consumer
        4 consumer写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.consumer_delete_request
            numbers = body.numbers
            identity = body.identity

            if not numbers:
                # TODO... 根据包体中的identity获取numbers
                pass

            # 发起请求的用户和要创建的用户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to delete consumer %s", self.numbers, numbers)
                self.code = 20510
                self.message = "no privilege to delete consumer"
                return 1

            self.code, self.message = consumer_delete_with_numbers(numbers)

            if 20500 == self.code:
                # 删除成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "delete consumer done"
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
    客户模块入口
    :param request: 解析后的pb格式
    :return: 0/不回包给前端，pb/正确返回，timeout/超时
    """
    try:
        consumer = Consumer(request)
        return consumer.enter()
    except Exception as e:
        g_log.error("%s", e)
        return 0


# pragma 增加用户资料API
def consumer_create(**kwargs):
    """
    增加用户资料
    :param kwargs: {"numbers": "18688982240", "nickname": "trace deng", "introduce": "ego cogito ergo sum",
                    "avatar": "", "qrcode": "", "email": "tracedeng@calculus.com", "country": "RPC", "location": "magic city",
                    "sexy": "female", "age": 18}
    :return: (20100, "yes")/成功，(>20100, "errmsg")/失败
    """
    try:
        # 检查要创建的用户numbers
        numbers = kwargs.get("numbers", "")
        if not user_is_valid_consumer(numbers):
            g_log.warning("invalid customer account %s", numbers)
            return 20101, "invalid phone number"

        # # 连接redis，检查该用户是否已经创建
        # connection = get_redis_connection(numbers)
        # if not connection:
        #     g_log.error("connect to redis failed")
        #     return 20102, "connect to redis failed"
        #
        # key = "user:%s" % numbers
        # if connection.exists(key) and connection.hget(key, "deleted") == "0":
        #     g_log.warning("duplicate create user %s", key)
        #     return 20103, "duplicate create user"

        # 昵称不能超过16字节，超过要截取前16字节
        nickname = kwargs.get("nickname", numbers)
        if len(nickname) > 32:
            g_log.warning("too long nickname %s", nickname)
            nickname = nickname[0:16]

        # 不合理的性别当作未知处理
        sexy = sexy_string_2_number(kwargs.get("sexy", "unknow"))

        age = kwargs.get("age", 0)
        if age > 500:
            g_log.warning("too old age %s", age)
            age = 500

        # 个人介绍不能超过512字节，超过要截取前512字节
        introduce = kwargs.get("introduce", "")
        if len(introduce) > 512:
            g_log.warning("too long introduce %s", introduce)
            introduce = introduce[0:512]

        # TODO... 头像、email、logo、国家、地区检查
        avatar = kwargs.get("avatar", "")
        email = kwargs.get("email", "")
        country = kwargs.get("country", "")
        location = kwargs.get("location", "")
        qrcode = kwargs.get("qrcode", "")

        value = {"numbers": numbers, "name": nickname, "avatar": avatar, "email": email, "introduce": introduce,
                 "sexy": sexy, "age": age, "country": country, "location": location, "qrcode": qrcode, "deleted": 0,
                 "create_time": datetime.now()}

        # 存入数据库
        collection = get_mongo_collection(numbers, "consumer")
        if not collection:
            g_log.error("get collection consumer failed")
            return 20102, "get collection consumer failed"
        consumer = collection.find_one_and_replace({"numbers": numbers}, value, upsert=True)
        if consumer and not consumer["deleted"]:
            g_log.error("consumer %s exist", numbers)
            return 20103, "duplicate consumer"
        # connection.hmset(key, value)
        # g_log.debug("insert %s %s", key, value)
        return 20100, "yes"
    # except (redis.ConnectionError, redis.TimeoutError) as e:
    #     g_log.error("connect to redis failed")
    #     return 20102, "connect to redis failed"
    except Exception as e:
        g_log.error("%s", e)
        return 20104, "exception"


# pragma 读取用户资料API
def consumer_retrieve_with_numbers(numbers):
    """
    读取用户资料
    :param numbers: 用户电话号码
    :return: (20200, consumer)/成功，(>20200, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_consumer(numbers):
            g_log.warning("invalid customer account %s", numbers)
            return 20201, "invalid phone number"

        # # 连接redis，检查该用户是否已经创建
        # connection = get_redis_connection(numbers)
        # if not connection:
        #     g_log.error("connect to redis failed")
        #     return 20202, "connect to redis failed"
        #
        # key = "user:%s" % numbers
        # if not connection.exists(key) or connection.hget(key, "deleted") == 1:
        #     g_log.warning("consumer %s not exist", key)
        #     return 20203, "consumer not exist"
        #
        # value = connection.hgetall(key)
        # g_log.debug("get %s %s", key, value)
        collection = get_mongo_collection(numbers, "consumer")
        if not collection:
            g_log.error("get collection consumer failed")
            return 20202, "get collection consumer failed"
        consumer = collection.find_one({"numbers": numbers, "deleted": 0})
        if not consumer:
            g_log.debug("consumer %s not exist", numbers)
            return 20203, "consumer not exist"

        return 20200, consumer
    # except (redis.ConnectionError, redis.TimeoutError) as e:
    #     g_log.error("connect to redis failed")
    #     return 20202, "connect to redis failed"
    except Exception as e:
        g_log.error("%s", e)
        return 20204, "exception"


def consumer_retrieve_with_identity(identity):
    """
    查询用户资料
    :param identity: 用户ID
    :return:
    """
    try:
        # 根据用户id查找用户电话号码
        numbers = ""
        return consumer_retrieve_with_numbers(numbers)
    except Exception as e:
        g_log.error("%s", e)
        return 20205, "exception"


def consumer_retrieve(numbers=None, identity=None):
    """
    获取用户资料，用户电话号码优先
    :param numbers: 用户电话号码
    :param identity: 用户ID
    :return:
    """
    try:
        if numbers:
            return consumer_retrieve_with_numbers(numbers)
        elif identity:
            return consumer_retrieve_with_identity(identity)
        else:
            return 20206, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 20207, "exception"


# pragma 更新用户资料API
def consumer_update_with_numbers(numbers, **kwargs):
    """
    更新用户资料
    :param numbers: 用户电话号码
    :param kwargs: {"numbers": "18688982240", "nickname": "trace deng", "introduce": "ego cogito ergo sum",
                    "avatar": "", "qrcode": "", "email": "tracedeng@calculus.com", "country": "RPC", "location": "magic city",
                    "sexy": "male", "age": 18}
    :return: (20400, "yes")/成功，(>20400, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_consumer(numbers):
            g_log.warning("invalid customer account %s", numbers)
            return 20401, "invalid phone number"

        # 连接redis，检查该用户是否已经创建
        connection = get_redis_connection(numbers)
        if not connection:
            g_log.error("connect to redis failed")
            return 20402, "connect to redis failed"

        # 检查账号是否存在
        key = "user:%s" % numbers
        if not connection.exists(key) or connection.hget(key, "deleted") == 1:
            g_log.warning("consumer %s not exist", key)
            return 20403, "consumer not exist"

        value = {}
        # 昵称不能超过16字节，超过要截取前16字节
        nickname = kwargs.get("nickname")
        if nickname and len(nickname) > 32:
            g_log.warning("too long nickname %s", nickname)
            nickname = nickname[0:16]
            value["nickname"] = nickname

        # 不合理的性别当作未知处理
        sexy = kwargs.get("sexy")
        if sexy:
            value["sexy"] = sexy_string_2_number(sexy)

        age = kwargs.get("age")
        if age:
            if age > 500:
                g_log.warning("too old age %s", age)
                age = 500
            value["age"] = age

        # 个人介绍不能超过512字节，超过要截取前512字节
        introduce = kwargs.get("introduce")
        if introduce and len(introduce) > 512:
            g_log.warning("too long introduce %s", introduce)
            introduce = introduce[0:512]
            value["introduce"] = introduce

        # TODO... 头像、email、logo、国家、地区检查
        avatar = kwargs.get("avatar")
        if avatar:
            value["avatar"] = avatar
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

        # 存入数据库
        connection.hmset(key, value)
        g_log.debug("insert %s %s", key, value)
        return 20400, "yes"
    except (redis.ConnectionError, redis.TimeoutError) as e:
        g_log.error("connect to redis failed")
        return 20405, "connect to redis failed"
    except Exception as e:
        g_log.error("%s", e)
        return 20406, "exception"


def consumer_update_with_identity(identity, **kwargs):
    """
    更用户资料
    :param identity: 用户ID
    :param kwargs: {"numbers": "18688982240", "nickname": "trace deng", "introduce": "ego cogito ergo sum",
                    "avatar": "", "qrcode": "", "email": "tracedeng@calculus.com", "country": "RPC", "location": "magic city",
                    "sexy": "male", "age": 18}
    :return:
    """
    try:
        # 根据用户id查找用户电话号码
        numbers = ""
        return consumer_update_with_numbers(numbers, **kwargs)
    except Exception as e:
        g_log.error("%s", e)
        return 20407, "exception"


def consumer_update(numbers=None, identity=None, **kwargs):
    """
    更新用户资料，用户电话号码优先
    :param numbers: 用户电话号码
    :param identity: 用户ID
    :param kwargs: {"numbers": "18688982240", "nickname": "trace deng", "introduce": "ego cogito ergo sum",
                    "avatar": "", "qrcode": "", "email": "tracedeng@calculus.com", "country": "RPC", "location": "magic city",
                    "sexy": "male", "age": 18}
    :return:
    """
    try:
        if numbers:
            return consumer_update_with_numbers(numbers, **kwargs)
        elif identity:
            return consumer_update_with_identity(identity, **kwargs)
        else:
            return 20408, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 20409, "exception"


# pragma 删除用户资料API
def consumer_delete_with_numbers(numbers):
    """
    删除用户资料
    :param numbers: 用户电话号码
    :return: (20500, "yes")/成功，(>20500, "errmsg")/失败
    """
    try:
        # 检查合法账号
        if not user_is_valid_consumer(numbers):
            g_log.warning("invalid customer account %s", numbers)
            return 20501, "invalid phone number"

        # # 连接redis，检查该用户是否已经创建
        # connection = get_redis_connection(numbers)
        # if not connection:
        #     g_log.error("connect to redis failed")
        #     return 20502, "connect to redis failed"
        #
        # # 检查账号是否存在
        # key = "user:%s" % numbers
        # if not connection.exists(key):
        #     g_log.warning("consumer %s not exist", key)
        #     return 20503, "consumer not exist"
        # value = connection.hget(key, "deleted")
        # g_log.debug("get %s#deleted %s", key, value)
        # if value == "1":
        #     g_log.warning("consumer %s deleted already", key)
        #     return 20504, "consumer not exist"
        #
        # # 删除consumer
        # connection.hset(key, "deleted", 1)

        collection = get_mongo_collection(numbers, "consumer")
        if not collection:
            g_log.error("get collection consumer failed")
            return 20502, "get collection consumer failed"
        consumer = collection.find_one_and_update({"numbers": numbers, "deleted": 0}, {"$set": {"deleted": 1}})
        if not consumer:
            g_log.error("consumer %s not exist", numbers)
            return 20503, "consumer not exist"
        return 20500, "yes"
    # except (redis.ConnectionError, redis.TimeoutError) as e:
    #     g_log.error("connect to redis failed")
    #     return 20505, "connect to redis failed"
    except Exception as e:
        g_log.error("%s", e)
        return 20506, "exception"


def consumer_delete_with_identity(identity):
    """
    删除用户资料
    :param identity: 用户ID
    :return:
    """
    try:
        # 根据用户id查找用户电话号码
        numbers = ""
        return consumer_delete_with_numbers(numbers)
    except Exception as e:
        g_log.error("%s", e)
        return 20507, "exception"


def consumer_delete(numbers=None, identity=None):
    """
    删除用户资料，用户电话号码优先
    :param numbers: 用户电话号码
    :param identity: 用户ID
    :return:
    """
    try:
        if numbers:
            return consumer_delete_with_numbers(numbers)
        elif identity:
            return consumer_delete_with_identity(identity)
        else:
            return 20508, "bad arguments"
    except Exception as e:
        g_log.error("%s", e)
        return 20509, "exception"


def consumer_material_copy_from_document(material, value):
    material.sexy = sexy_number_2_string(value["sexy"])  # 从redis取出来的值都是字符串
    material.age = int(value["age"])
    material.introduce = value["introduce"]
    material.email = value["email"]
    material.nickname = value["name"]
    material.location = value["location"]
    material.country = value["country"]
    material.qrcode = value["qrcode"]
    material.avatar = value["avatar"]
    material.create_time = value["create_time"].strftime("%Y-%m-%d %H:%M:%S")