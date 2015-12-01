# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import string
import random
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.collection import ReturnDocument
from qiniu import Auth
import common_pb2
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
import package
from account_valid import account_is_valid_merchant, account_is_valid_consumer
from account import identity_to_numbers
from consumer import consumer_update_with_numbers
from merchant import merchant_update_with_numbers

g_access_key = "-dQq5iRtWUy6LAaaY63DqXcC_qS1lscT2oCuejt9"
g_secret_key = "2-3d9wfd6UdBaviz6934NzePY6g_MHKzPg4pJZtC"
g_bucket_name = "calculus"
g_bucket_name_debug = "calculus"
g_qiniu_callback_url = "http://localhost"


class QiniuToken():
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
            command_handle = {601: self.upload_token_retrieve, 602: self.download_token_retrieve,
                              603: self.access_token_retrieve}
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

    def upload_token_retrieve(self):
        """
        获取上传token
        """
        try:
            body = self.request.upload_token_request
            numbers = body.numbers
            identity = body.identity
            debug = body.debug
            kind = body.resource_kind
            merchant_identity = body.merchant_identity

            if not numbers:
                # 根据包体中的identity获取numbers
                code, numbers = identity_to_numbers(identity)
                if code != 10500:
                    self.code = 70101
                    self.message = "missing argument"
                    return 1

            # 发起请求的用户和要获取的用户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to retrieve upload token for %s", self.numbers, numbers)
                self.code = 70102
                self.message = "no privilege to retrieve upload token"
                return 1

            kwargs = {"numbers": numbers, "kind": kind, "debug": debug, "merchant_identity": merchant_identity}
            self.code, self.message = upload_token_retrieve(**kwargs)

            if 70100 == self.code:
                # 获取成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "retrieve upload token done"

                body = response.upload_token_response
                body.upload_token = self.message
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def download_token_retrieve(self):
        """
        获取下载token
        """
        try:
            body = self.request.download_token_request
            numbers = body.numbers
            identity = body.identity

            if not numbers:
                # 根据包体中的identity获取numbers
                code, numbers = identity_to_numbers(identity)
                if code != 10500:
                    self.code = 70201
                    self.message = "missing argument"
                    return 1

            # 发起请求的用户和要获取的用户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to retrieve download token for %s", self.numbers, numbers)
                self.code = 70202
                self.message = "no privilege to retrieve download token"
                return 1

            self.code, self.message = download_token_retrieve(numbers)

            if 70200 == self.code:
                # 获取成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "retrieve download token done"

                body = response.download_token_response
                body.upload_token = self.message
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def access_token_retrieve(self):
        """
        获取资源管理token
        """
        try:
            body = self.request.access_token_request
            numbers = body.numbers
            identity = body.identity

            if not numbers:
                # 根据包体中的identity获取numbers
                code, numbers = identity_to_numbers(identity)
                if code != 10500:
                    self.code = 70301
                    self.message = "missing argument"
                    return 1

            # 发起请求的用户和要获取的用户不同，认为没有权限，TODO...更精细控制
            if self.numbers != numbers:
                g_log.warning("%s no privilege to retrieve access token for %s", self.numbers, numbers)
                self.code = 70302
                self.message = "no privilege to retrieve access token"
                return 1

            self.code, self.message = upload_token_retrieve(numbers)

            if 70300 == self.code:
                # 获取成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "retrieve access token done"

                body = response.access_token_response
                body.upload_token = self.message
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
    7牛token模块入口
    :param request: 解析后的pb格式
    :return: 0/不回包给前端，pb/正确返回，timeout/超时
    """
    try:
        qiniu_token = QiniuToken(request)
        return qiniu_token.enter()
    except Exception as e:
        g_log.error("%s", e)
        return 0


def upload_token_retrieve(**kwargs):
    """
    上传凭证
    :param kwargs: {"numbers": 18688982240, "kind": "m_logo", "merchant_identity": "562c7ad6494ac55faf750798"}
    :return:
    """
    try:
        numbers = kwargs.get("numbers", "")
        debug = kwargs.get("debug", "online")
        kind = kwargs.get("kind", "dummy")
        merchant_identity = kwargs.get("merchant_identity", "")
        if kind == "m_logo" and not merchant_identity:
            g_log.warning("missing merchant identity")
            return 70121, "missing argument"

        if debug == "debug":
            # 测试
            return upload_token_retrieve_debug(kind, numbers, merchant_identity)
        else:
            # online
            return upload_token_retrieve_online(kind, numbers, merchant_identity)

    except Exception as e:
        g_log.error("%s %s", e.__class__, e)
        return 70122, "exception"


def upload_token_retrieve_online(kind, numbers, merchant_identity=""):
    """
    线上环境生成上传凭证
    平台回调完成，生成资源下载key，并存入平台数据库
    回调返回数据结构
    {
        "key": "key",
        "payload": {
            {"success":true,"name":"key"}
        }
    }
    :param kind: 上传的资源类型
    :param numbers: 账号
    :return:
    """
    # 7牛环境初始化
    q = Auth(g_access_key, g_secret_key)

    # 回调url
    kind_to_path = {"c_avatar": "consumer", "m_logo": "merchant", "ma_poster": "activity"}
    callback_url = "%s/%s" % (g_qiniu_callback_url, kind_to_path.get(kind, "dummy"))

    # 回调post参数
    kind_to_type = {"c_avatar": "update_avatar", "m_logo": "update_logo", "ma_poster": "update_poster"}
    callback_body_type = kind_to_type.get(kind, "dummy")
    callback_body = {"type": callback_body_type, "numbers": numbers, "merchant": merchant_identity, "hash": "$(etags)"}

    policy = {"callbackUrl": callback_url, "callbackBody": callback_body, "callbackBodyType": "application/json",
              "callbackFetchKey": 1, "mimeLimit": "image/*"}

    upload_token = q.upload_token(g_bucket_name, expires=3600, policy=policy)
    g_log.debug("debug upload token, [policy:%s, token:%s]", policy, upload_token)

    # 修改资料图片路径
    if kind == "c_avatar":
        # 平台修改客户头像资料
        if not account_is_valid_consumer(numbers):
            g_log.warning("%s invalid consumer account", numbers)
            return 70117, "invalid consumer account"
    elif kind == "m_logo":
        # 平台修改商家logo资料
        if not account_is_valid_merchant(numbers):
            return 70118, "invalid consumer account"
    elif kind == "m_activity_poster":
        # 平台修改活动图片资料
        if not account_is_valid_merchant(numbers):
            return 70119, "invalid consumer account"
    else:
        g_log.debug("unsupported resource kind %s", kind)
        return 70120, "unsupported resource kind"

    return 70100, upload_token


def upload_token_retrieve_debug(kind, numbers, merchant_identity=""):
    """
    debug环境生成上传凭证
    指定客户头像路径c/avatar/numbers, 商家logo路径m/logo/numbers, 商家活动路径ma/poster/numbers
    :param kind: 资源类型
    :param numbers: 账号
    :return:
    """
    q = Auth(g_access_key, g_secret_key)

    kind_to_key = {"c_avatar": "c/avatar", "m_logo": "m/logo", "ma_poster": "ma/poster"}
    key = "%s/%s" % (kind_to_key.get(kind, "dummy"), numbers)
    policy = {"mimeLimit": "image/*"}
    upload_token = q.upload_token(g_bucket_name_debug, key=key, expires=3600, policy=policy)
    g_log.debug("debug upload token, [key:%s, policy:%s, token:%s]", key, policy, upload_token)

    # 修改资料图片路径
    if kind == "c_avatar":
        # 平台修改客户头像资料
        if not account_is_valid_consumer(numbers):
            g_log.warning("%s invalid consumer account", numbers)
            return 70111, "invalid consumer account"
        code, message = consumer_update_with_numbers(numbers, avatar=key)
        if code != 20400:
            g_log.warning("update consumer %s avatar %s failed", numbers, key)
            return 70112, "update consumer avatar failed"
    elif kind == "m_logo":
        # 平台修改商家logo资料
        if not account_is_valid_merchant(numbers):
            return 70113, "invalid merchant account"
        if not merchant_identity:
            return 70114, "missing merchant identity"
        code, message = merchant_update_with_numbers(numbers, merchant_identity, logo=key)
        if code != 30400:
            g_log.warning("update merchant logo %s failed", key)
            return 70114, "update merchant logo failed"
    elif kind == "m_activity_poster":
        # 平台修改活动图片资料
        if not account_is_valid_merchant(numbers):
            return 70115, "invalid merchant account"
        pass
    else:
        g_log.debug("unsupported resource kind %s", kind)
        return 70116, "unsupported resource kind"

    return 70100, upload_token