# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import redis
from redis_connection import get_redis_connection
import common_pb2
import package
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
import account_valid


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
            command_handle = {100: self.consumer_create, 101: self.consumer_retrieve, 102: self.consumer_batch_retrieve,
                              103: self.consumer_update, 104: self.consumer_delete}
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
            phone_number = body.phone_number
            material = body.material

            # 检查要创建的用户phone_number
            if phone_number == "":
                # 如果包体中没有phone_number，则用material中的
                phone_number = material.phone_number
            if account_valid.user_is_valid_consumer(phone_number) == 0:
                g_log.warning("invalid customer account %s", phone_number)
                # TODO... 根据包体中的consumer_identity获取phone_number
                self.code = 20101
                self.message = "invalid phone number"
                return 1
            # 发起请求的用户和要创建的用户不同，认为没有权限，TODO...更精细控制
            if self.phone_number != phone_number:
                g_log.warning("%s no privilege to create consumer %s", self.phone_number, phone_number)
                self.code = 20104
                self.message = "no privilege to create consumer"
                return 1

            # 连接redis，检查该用户是否已经创建
            connection = get_redis_connection(phone_number)
            if not connection:
                g_log.error("connect to redis failed")
                self.code = 20102
                self.message = "connect to redis failed"
                return 1
            key = "user:%s" % phone_number
            if connection.exists(key) and connection.hget(key, "deleted") == "0":
                    g_log.warning("duplicate create user %s", key)
                    self.code = 20103
                    self.message = "duplicate create user"
                    return 1

            # 昵称不能超过16字节，超过要截取前16字节
            nickname = material.nickname
            if len(nickname) > 16:
                g_log.warning("too long nickname %s", material.nickname)
                nickname = nickname[0:16]

            # 不合理的性别当作未知处理
            sexy = material.sexy
            if sexy > 2:
                g_log.warning("invalid sexy %s, consider unknow sexy", material.sexy)
                sexy = 0

            age = material.age
            if age > 500:
                g_log.warning("too old age %s", age)
                age = 500

            # TODO... email检查
            email = material.email

            # 个人介绍不能超过64字节，超过要截取前64字节
            introduce = material.introduce
            if len(introduce) > 64:
                g_log.warning("too long introduce %s", introduce)
                introduce = introduce[0:64]

            # 国家、地区检查
            country = material.country
            location = material.location

            value = {"nickname": nickname, "sexy": sexy, "age": age, "email": email, "introduce": introduce,
                     "country": country, "location": location, "deleted": 0}

            # 存入数据库
            connection.hmset(key, value)
            g_log.debug("insert %s %s", key, value)

            # 返回结果
            response = common_pb2.Response()
            response.head.cmd = self.head.cmd
            response.head.seq = self.head.seq
            response.head.code = 1
            response.head.message = "create consumer done"
            # g_log.debug("%s", response)
            return response
        except (redis.ConnectionError, redis.TimeoutError) as e:
            g_log.error("connect to redis failed")
            self.code = 20102
            self.message = "connect to redis failed"
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
        4 从数据库读取用户资料
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.consumer_retrieve_request
            phone_number = body.phone_number

            # 检查要获取资料的用户phone_number
            if phone_number == "" or account_valid.user_is_valid_consumer(phone_number) == 0:
                # 如果包体中没有或者无效的phone_number
                g_log.warning("invalid customer account %s", phone_number)
                # TODO... 根据包体中的consumer_identity获取phone_number
                self.code = 20201
                self.message = "invalid phone number"
                return 1
            # 发起请求的用户和要创建的用户不同，认为没有权限，TODO...更精细控制
            if self.phone_number != phone_number:
                g_log.warning("%s no privilege to retrieve consumer %s", self.phone_number, phone_number)
                self.code = 20204
                self.message = "no privilege to retrieve consumer"
                return 1

            # 连接redis，检查该用户是否已经创建
            connection = get_redis_connection(self.phone_number)
            if not connection:
                g_log.error("connect to redis failed")
                self.code = 20202
                self.message = "connect to redis failed"
                return 1
            key = "user:%s" % phone_number
            if not connection.exists(key) or connection.hget(key, "deleted") == 1:
                g_log.warning("consumer %s not exist", key)
                self.code = 20203
                self.message = "consumer not exist"
                return 1

            value = connection.hgetall(key)
            g_log.debug("get %s %s", key, value)

            # 返回结果
            response = common_pb2.Response()
            response.head.cmd = self.head.cmd
            response.head.seq = self.head.seq
            response.head.code = 1
            response.head.message = "retrieve consumer done"

            material = response.consumer_retrieve_response.material
            material.phone_number = phone_number
            material.sexy = int(value["sexy"])
            material.age = int(value["age"])
            material.introduce = value["introduce"]
            material.email = value["email"]
            material.nickname = value["nickname"]
            material.location = value["location"]
            material.country = value["country"]
            # material.qrcode = value["qrcode"]
            # material.head_image = value["image"]
            return response
        except (redis.ConnectionError, redis.TimeoutError) as e:
            g_log.error("connect to redis failed")
            self.code = 20102
            self.message = "connect to redis failed"
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
            phone_number = body.phone_number
            material = body.material

            # 检查要创建的用户phone_number
            if phone_number == "":
                # 如果包体中没有phone_number，则用material中的
                phone_number = material.phone_number
            if account_valid.user_is_valid_consumer(phone_number) == 0:
                g_log.warning("invalid customer account %s", phone_number)
                # TODO... 根据包体中的consumer_identity获取phone_number
                self.code = 20301
                self.message = "invalid phone number"
                return 1
            # 发起请求的用户和要创建的用户不同，认为没有权限，TODO...更精细控制
            if self.phone_number != phone_number:
                g_log.warning("%s no privilege to update consumer %s", self.phone_number, phone_number)
                self.code = 20304
                self.message = "no privilege to update consumer"
                return 1

            # 连接redis，检查该用户是否已经创建
            connection = get_redis_connection(phone_number)
            if not connection:
                g_log.error("connect to redis failed")
                self.code = 20302
                self.message = "connect to redis failed"
                return 1
            key = "user:%s" % phone_number
            if not connection.exists(key) or connection.hget(key, "deleted") == "1":
                g_log.warning("consumer %s not exist", key)
                self.code = 20303
                self.message = "consumer not exist"
                return 1

            # g_log.debug(dir(material))
            # g_log.debug(material.ListFields()[0][0].name)
            # import google.protobuf.descriptor
            # self.request.consumer_update_request.HasField("material")
            # self.request.consumer_update_request.material.HasFile('nickname')
            # TODO... HasField无效
            value = {}
            if material.HasField('nickname'):
                # 昵称不能超过16字节，超过要截取前16字节
                nickname = material.nickname
                if len(nickname) > 16:
                    g_log.warning("too long nickname %s", material.nickname)
                    nickname = nickname[0:16]
                value["nickname"] = nickname

            if material.HasField("sexy"):
                # 不合理的性别当作未知处理
                sexy = material.sexy
                if sexy > 2:
                    g_log.warning("invalid sexy %s, consider unknow sexy", material.sexy)
                    sexy = 0
                value["sexy"] = sexy

            if material.HasField("age"):
                age = material.age
                if age > 500:
                    g_log.warning("too old age %s", age)
                    age = 500
                value["age"] = age

            if material.HasField("email"):
                # TODO... email检查
                email = material.email
                value["email"] = email

            if material.HasField("age"):
                # 个人介绍不能超过64字节，超过要截取前64字节
                introduce = material.introduce
                if len(introduce) > 64:
                    g_log.warning("too long introduce %s", introduce)
                    introduce = introduce[0:64]
                value["introduce"] = introduce

            # 国家、地区检查
            if material.HasField("country"):
                country = material.country
                value["country"] = country
            if material.HasField("location"):
                location = material.location
                value["location"] = location

            # 更新数据库
            connection.hmset(key, value)
            g_log.debug("insert %s %s", key, value)

            # 返回结果
            response = common_pb2.Response()
            response.head.cmd = self.head.cmd
            response.head.seq = self.head.seq
            response.head.code = 1
            response.head.message = "update consumer done"
            # g_log.debug("%s", response)
            return response
        except (redis.ConnectionError, redis.TimeoutError) as e:
            g_log.error("connect to redis failed")
            self.code = 20302
            self.message = "connect to redis failed"
            return 1
        except Exception as e:
            g_log.error("<%s> %s", e.__class__.__name__, e)
            # g_log.error(e.__class__)
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
            phone_number = body.phone_number

            # 检查要获取资料的用户phone_number
            if phone_number == "" or account_valid.user_is_valid_consumer(phone_number) == 0:
                # 如果包体中没有或者无效的phone_number
                g_log.warning("invalid customer account %s", phone_number)
                # TODO... 根据包体中的consumer_identity获取phone_number
                self.code = 20501
                self.message = "invalid phone number"
                return 1
            # 发起请求的用户和要创建的用户不同，认为没有权限，TODO...更精细控制
            if self.phone_number != phone_number:
                g_log.warning("%s no privilege to delete consumer %s", self.phone_number, phone_number)
                self.code = 20504
                self.message = "no privilege to delete consumer"
                return 1

            # 连接redis，检查该用户是否已经创建
            connection = get_redis_connection(self.phone_number)
            if not connection:
                g_log.error("connect to redis failed")
                self.code = 20502
                self.message = "connect to redis failed"
                return 1
            key = "user:%s" % phone_number
            if not connection.exists(key):
                g_log.warning("consumer %s not exist", key)
                self.code = 20503
                self.message = "consumer not exist"
                return 1

            value = connection.hget(key, "deleted")
            g_log.debug("get %s#deleted %s", key, value)

            if value == "1":
                g_log.warning("consumer %s deleted already", key)
                self.code = 20505
                self.message = "consumer not exist"
                return 1

            # 删除consumer
            connection.hset(key, "deleted", 1)

            # 返回结果
            response = common_pb2.Response()
            response.head.cmd = self.head.cmd
            response.head.seq = self.head.seq
            response.head.code = 1
            response.head.message = "delete consumer done"
            return response
        except (redis.ConnectionError, redis.TimeoutError) as e:
            g_log.error("connect to redis failed")
            self.code = 20102
            self.message = "connect to redis failed"
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
        consumer = Consumer(request)
        return consumer.enter()
    except Exception as e:
        g_log.error("%s", e)
        return 0