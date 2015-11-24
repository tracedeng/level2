# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

from mongo_connection import get_mongo_collection
import time
from datetime import datetime
import base64
import hashlib
import common_pb2
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
import package
from account_valid import *
from xxtea import decrypt, encrypt


class Account():
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
        try:
            command_handle = {2: self.login_request, 3: self.register_request, 4: self.change_password_request}
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

    def login_request(self):
        """
        登录请求
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.login_request
            phone_number = body.phone_number
            password_md5 = body.password_md5

            kwargs = {"numbers": phone_number, "password_md5": password_md5}
            g_log.debug("login request: %s", kwargs)
            self.code, self.message = login_request(**kwargs)
            if 10100 == self.code:
                # 登录成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "login done"

                response.login_response.session_key = self.message
                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def register_request(self):
        """
        注册请求
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.register_request
            phone_number = body.phone_number
            password = body.password
            password_md5 = body.password_md5

            kwargs = {"numbers": phone_number, "password": password, "password_md5": password_md5}
            g_log.debug("register request: %s", kwargs)
            self.code, self.message = register_request(**kwargs)
            if 10200 == self.code:
                # 注册成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "account register done"

                return response
            else:
                return 1
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def change_password_request(self):
        """
        重置密码
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """
        try:
            body = self.request.change_password_request
            phone_number = body.phone_number
            password = body.password
            password_md5 = body.password_md5

            kwargs = {"numbers": phone_number, "password": password, "password_md5": password_md5}
            self.code, self.message = change_password_request(**kwargs)
            if 10300 == self.code:
                # 重置成功
                response = common_pb2.Response()
                response.head.cmd = self.head.cmd
                response.head.seq = self.head.seq
                response.head.code = 1
                response.head.message = "change password done"

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
        account = Account(request)
        return account.enter()
    except Exception as e:
        g_log.error("%s", e)
        return 0


def generate_session_key(timestamp, numbers):
    """
    session_key = base64_encode(tea(timestamp + numbers))
    10位时间 ＋ 账号
    :param timestamp: 时间挫，生产session_key的时间
    :param numbers: 账号
    :return: session_key/成功，None/失败
    """
    try:
        key = "key"
        plain = "%s%s" % (timestamp, numbers)
        session_key = base64.b64encode(encrypt(plain, key))
        return session_key
    except Exception as e:
        g_log.error("<%s> %s", e.__class__, e)
        return None


def decrypt_session_key(session_key):
    """
    解码session_key
    :param session_key: session key
    :return: (timestamp, numbers)/成功，None/失败
    """
    try:
        key = "key"
        cipher = session_key
        plain = decrypt(base64.b64decode(cipher), key)
        if not plain:
            return None
        timestamp = plain[0:10]
        numbers = plain[10:]
        return int(timestamp), numbers
    except Exception as e:
        g_log.error("<%s> %s", e.__class__, e)
        return None


def login_request(**kwargs):
    """
    账号登录
    :param kwargs: {"numbers": "18688982240", "password_md5": "c56d0e9a7ccec67b4ea131655038d604"}
    :return: (10100, "yes")/成功，(>10100, "errmsg")/失败
    """
    try:
        # 检查要创建的用户numbers
        numbers = kwargs.get("numbers", "")
        if not account_is_valid(numbers):
            g_log.warning("invalid account %s", numbers)
            return 10101, "invalid account"

        password_md5 = kwargs.get("password_md5", "")

        # 验证密码
        collection = get_mongo_collection(numbers, "account")
        if not collection:
            g_log.error("get collection account failed")
            return 10103, "get collection account failed"
        account = collection.find_one({"phone_numbers": numbers, "password_md5": password_md5, "deleted": 0})
        if not account:
            g_log.debug("account %s not exist or illegal password", numbers)
            return 10104, "illegal password"

        # 生成session key
        timestamp = int(time.time())
        session_key = generate_session_key(str(timestamp), numbers)
        if not session_key:
            g_log.error("generate session key failed")
            return 10105, "gen session key failed"

        value = {"numbers": numbers, "session_key": session_key, "create_time": datetime.fromtimestamp(timestamp),
                 "active_time": datetime.fromtimestamp(timestamp)}
        # session 入库
        # TODO... 如果登录模块分离，则考虑session存入redis
        collection = get_mongo_collection(numbers, "session")
        if not collection:
            g_log.error("get collection session failed")
            return 10106, "get collection session failed"
        session = collection.find_one_and_update({"numbers": numbers}, {"$set": value})
        # 第一次更新，则插入一条
        if not session:
            g_log.debug("first login")
            session = collection.insert_one(value)
        if not session:
            g_log.error("%s login failed", numbers)
            return 10107, "login failed"
        g_log.debug("login succeed")

        return 10100, session_key
    except Exception as e:
        g_log.error("%s", e)
        return 10108, "exception"


def check_md5(plain, cipher, times):
    """
    检查 cipher = md5(plain) * times，
    :param plain: 明文
    :param cipher: 密文
    :param times: 几次md5
    :return:
    """
    try:
        for i in xrange(0, times):
            m = hashlib.md5()
            m.update(plain)
            plain = m.hexdigest()

        if plain == cipher:
            return 0
        else:
            return 1
    except Exception as e:
        g_log.error("<%s> %s", e.__class__, e)
        return 1


def register_request(**kwargs):
    """
    账号注册
    :param kwargs: {"numbers": "18688982240", "password": "123456", "password_md5": "c56d0e9a7ccec67b4ea131655038d604"}
    :return: (10200, "yes")/成功，(>10200, "errmsg")/失败
    """
    try:
        # 检查要创建的用户numbers
        numbers = kwargs.get("numbers", "")
        if not account_is_valid(numbers):
            g_log.warning("invalid account %s", numbers)
            return 10201, "invalid account"

        # TODO... 检查密码字符
        password = kwargs.get("password", "")
        password_md5 = kwargs.get("password_md5", "")

        # password_md5 = md5(md5(md5(password)))
        if 1 == check_md5(password, password_md5, 3):
            g_log.warning("password_md5 != md5(md5(md5(password)))")
            return 10202, "invalid password"

        value = {"phone_numbers": numbers, "password": password, "password_md5": password_md5,
                 "deleted": 0, "time": datetime.now()}
        # 存入数据库
        collection = get_mongo_collection(numbers, "account")
        if not collection:
            g_log.error("get collection account failed")
            return 10204, "get collection account failed"
        account = collection.find_one_and_update({"phone_numbers": numbers}, {"$set": value})

        # 第一次更新，则插入一条
        if not account:
            g_log.debug("register new account")
            account = collection.insert_one(value)
        if not account:
            g_log.error("register account %s failed", numbers)
            return 10205, "register account failed"
        g_log.debug("register succeed")

        return 10200, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 10206, "exception"


def change_password_request(**kwargs):
    """
    重置密码
    :param kwargs: {"numbers": "18688982240", "password": "123456", "password_md5": "c56d0e9a7ccec67b4ea131655038d604"}
    :return: (10300, "yes")/成功，(>10300, "errmsg")/失败
    """
    try:
        # 检查要创建的用户numbers
        numbers = kwargs.get("numbers", "")
        if not account_is_valid(numbers):
            g_log.warning("invalid account %s", numbers)
            return 10301, "invalid account"

        # TODO... 检查密码字符
        password = kwargs.get("password", "")
        password_md5 = kwargs.get("password_md5", "")

        # password_md5 = md5(md5(md5(password)))
        if 1 == check_md5(password, password_md5, 3):
            g_log.warning("password_md5 != md5(md5(md5(password)))")
            return 10302, "invalid password"

        value = {"phone_numbers": numbers, "password": password, "password_md5": password_md5,
                 "deleted": 0, "update_time": datetime.now()}
        # 更新数据库
        collection = get_mongo_collection(numbers, "account")
        if not collection:
            g_log.error("get collection account failed")
            return 10304, "get collection account failed"
        account = collection.find_one_and_update({"phone_numbers": numbers}, {"$set": value})
        if not account:
            g_log.error("account %s change password failed", numbers)
            return 10305, "change password failed"
        g_log.debug("change password succeed")

        return 10300, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 10306, "exception"


def verify_session_key(session_key, account):
    """
    验证session key
    :param session_key: session key
    :param account: 账号
    :return:
    """
    try:
        # session 算法验证
        plain = decrypt_session_key(session_key)
        g_log.debug(plain)
        if not plain:
            g_log.error("illegal session key %s", session_key)
            return 10401, "illegal session key"
        (timestamp, numbers) = plain
        if account != numbers:
            g_log.error("not account %s session", account)
            return 10402, "account not match"

        # session 落地验证
        collection = get_mongo_collection(numbers, "session")
        if not collection:
            g_log.error("get collection session failed")
            return 10403, "get collection session failed"
        create_time = datetime.fromtimestamp(timestamp)
        session = collection.find_one({"numbers": numbers, "session_key": session_key, "create_time": create_time})
        if not session:
            g_log.debug("session %s not exist", session_key)
            return 10404, "session not exist"

        # TODO... 比较活跃时间
        g_log.debug("last active time %s", session["active_time"])
        # 更新活跃时间
        active_time = datetime.now()
        session = collection.find_one_and_update({"numbers": numbers, "session_key": session_key},
                                                 {"$set": {"active_time": active_time}})
        if not session:
            g_log.warning("update active time failed")

        return 10400, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 10405, "exception"


if "__main__" == __name__:
    result1 = verify_session_key("7KiWrPOQgmvMpjsIVIVmr0ulNYFN4vSfWCKjGg==", "18688982240")
    g_log.debug(result1)