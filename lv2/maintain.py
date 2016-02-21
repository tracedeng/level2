# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

from datetime import datetime
from pymongo.collection import ReturnDocument
from mongo_connection import get_mongo_collection
import common_pb2
from bson.objectid import ObjectId
import package
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from account_valid import account_is_valid_consumer, account_is_valid
from account_auxiliary import verify_session_key, identity_to_numbers


class Maintain():
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
            if self.cmd != 902:
                # 验证登录态，某些命令可能不需要登录态，此处做判断
                code, message = verify_session_key(self.numbers, self.session_key)
                if 10400 != code:
                    g_log.debug("verify session key failed, %s, %s", code, message)
                    return package.error_response(self.cmd, self.seq, 90001, "invalid session key")

            command_handle = {901: self.version_report, 902: self.boot_report,
                              903: self.active_report, 904: self.feed_back}

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

    def version_report(self):
        """
        版本上报
        :return:
        """
        try:
            body = self.request.version_report_request
            numbers = body.numbers
            version = body.version
            identity = body.identity

            if not numbers:
                # 根据包体中的merchant_identity获取numbers
                code, numbers = identity_to_numbers(identity)
                if code != 10500:
                    self.code = 90101
                    self.message = "missing argument"
                    return 1
                
            self.code, self.message = version_report(numbers, version)

            if 90100 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "version report done"

                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def boot_report(self):
        """
        启动上报
        :return:
        """
        try:
            body = self.request.boot_report_request
            # numbers = body.numbers
            version = body.version
            # identity = body.identity

            # if not numbers:
            #     # 根据包体中的merchant_identity获取numbers
            #     code, numbers = identity_to_numbers(identity)
            #     if code != 10500:
            #         self.code = 90201
            #         self.message = "missing argument"
            #         return 1

            self.code, self.message = boot_report(version)

            if 90200 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "boot report done"

                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def active_report(self):
        """
        活跃上报
        :return:
        """
        try:
            body = self.request.active_report_request
            numbers = body.numbers
            identity = body.identity
            mode = body.mode

            if not numbers:
                # 根据包体中的merchant_identity获取numbers
                code, numbers = identity_to_numbers(identity)
                if code != 10500:
                    self.code = 90301
                    self.message = "missing argument"
                    return 1

            self.code, self.message = active_report(numbers, mode)

            if 90300 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "active report done"

                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def feed_back(self):
        """
        反馈
        :return:
        """
        try:
            body = self.request.feed_back_request
            numbers = body.numbers
            identity = body.identity
            version = body.version
            mode = body.mode
            feedback = body.feedback

            if not numbers:
                # 根据包体中的merchant_identity获取numbers
                code, numbers = identity_to_numbers(identity)
                if code != 10500:
                    self.code = 90401
                    self.message = "missing argument"
                    return 1

            kwargs = {"numbers": numbers, "version": version, "mode": mode, "feedback": feedback}
            self.code, self.message = feed_back(**kwargs)

            if 90400 == self.code:
                # 更新成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "feed back done"

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
        maintain = Maintain(request)
        return maintain.enter()
    except Exception as e:
        g_log.error("%s", e)
        return 0


def version_report(numbers, version):
    """
    版本上报
    :param numbers:
    :param version: 版本
    :return:
    """
    try:
        # 检查合法账号
        if not account_is_valid(numbers):
            g_log.warning("invalid customer account %s", numbers)
            return 90111, "invalid account"

        collection = get_mongo_collection("version")
        if not collection:
            g_log.error("get collection version failed")
            return 90113, "get collection version failed"

        value = {"numbers": numbers, "version": version, "feedback_time": datetime.now()}
        version_identity = collection.insert_one(value).inserted_id
        version_identity = str(version_identity)
        g_log.debug("insert version %s", version_identity)

        return 90100, version_identity
    except Exception as e:
        g_log.error("%s", e)
        return 90115, "exception"


def boot_report(numbers, version):
    """
    启动上报
    :param numbers:
    :param version: 版本
    :return:
    """
    try:
        # 检查合法账号
        # if not account_is_valid(numbers):
        #     g_log.warning("invalid customer account %s", numbers)
        #     return 90311, "invalid account"

        collection = get_mongo_collection("boot")
        if not collection:
            g_log.error("get collection boot failed")
            return 90213, "get collection boot failed"

        boot_time = datetime.now()
        value = {"numbers": numbers, "version": version, "boot_time": boot_time, "total": 1}
        boot = collection.find_one_and_update({"boot_time": boot_time, "version": version}, {"$inc": {"total": 1}},
                                              {"$set": value})
        if not boot:
            g_log.error("boot report failed")
            return 50714, "boot report failed"

        return 90200, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 90215, "exception"


def active_report(numbers, mode):
    """
    活跃上报
    :param numbers:
    :param mode: merchant or consumer
    :return:
    """
    try:
        # 检查合法账号
        if not account_is_valid(numbers):
            g_log.warning("invalid customer account %s", numbers)
            return 90311, "invalid account"

        collection = get_mongo_collection("active")
        if not collection:
            g_log.error("get collection active failed")
            return 90313, "get collection active failed"

        value = {"numbers": numbers, "mode": mode, "active_time": datetime.now()}
        active_identity = collection.insert_one(value).inserted_id
        active_identity = str(active_identity)
        g_log.debug("insert active report %s", active_identity)

        return 90300, active_identity
    except Exception as e:
        g_log.error("%s", e)
        return 90315, "exception"


def feed_back(**kwargs):
    """
    反馈
    :param kwargs: {"numbers": 186889882240, "version": "1.0", "mode": "consumer", "feedback": "bug bug bug bug bug"}
    :return:
    """
    try:
        # 检查合法账号
        numbers = kwargs.get("numbers", "")
        if not account_is_valid(numbers):
            g_log.warning("invalid account %s", numbers)
            return 90411, "invalid account"

        version = kwargs.get("version", "N/A")
        mode = kwargs.get("mode", "consumer")
        feedback = kwargs.get("feedback", "")

        value = {"numbers": numbers, "version": version, "mode": mode, "feedback": feedback,
                 "feedback_time": datetime.now()}

        collection = get_mongo_collection("feedback")
        if not collection:
            g_log.error("get collection feedback failed")
            return 90412, "get collection feedback failed"

        feedback_identity = collection.insert_one(value).inserted_id
        feedback_identity = str(feedback_identity)
        g_log.debug("insert feedback %s", feedback_identity)

        return 90400, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 90413, "exception"


def voucher_copy_from_document(material, value):
    material.activity_identity = value["activity_identity"]
    material.used = int(value["used"])
    material.activity_title = value["activity_title"]
    material.create_time = value["create_time"].strftime("%Y-%m-%d %H:%M:%S")
    material.expire_time = value["expire_time"].strftime("%Y-%m-%d %H:%M:%S")
    material.identity = str(value["_id"])


# 测试时mongo_connection的配置文件路径写全
# if "__main__" == __name__:
