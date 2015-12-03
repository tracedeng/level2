# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

from mongo_connection import get_mongo_collection
from bson.objectid import ObjectId
import time
from datetime import datetime
import base64
import binascii
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
            numbers = body.numbers
            password_md5 = body.password_md5

            kwargs = {"numbers": numbers, "password_md5": password_md5}
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
            numbers = body.numbers
            password = body.password
            password_md5 = body.password_md5

            kwargs = {"numbers": numbers, "password": password, "password_md5": password_md5}
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
            numbers = body.numbers
            password = body.password
            password_md5 = body.password_md5

            kwargs = {"numbers": numbers, "password": password, "password_md5": password_md5}
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
    :return: (10100, "yes")/成功，(>10110, "errmsg")/失败
    """
    try:
        # 检查要创建的用户numbers
        numbers = kwargs.get("numbers", "")
        if not account_is_valid(numbers):
            g_log.warning("invalid account %s", numbers)
            return 10111, "invalid account"

        password_md5 = kwargs.get("password_md5", "")

        # 验证密码
        collection = get_mongo_collection("account")
        if not collection:
            g_log.error("get collection account failed")
            return 10113, "get collection account failed"
        account = collection.find_one({"numbers": numbers, "password_md5": password_md5, "deleted": 0})
        if not account:
            g_log.debug("account %s not exist or illegal password", numbers)
            return 10114, "illegal password"

        # 生成session key
        timestamp = int(time.time())
        session_key = generate_session_key(str(timestamp), numbers)
        if not session_key:
            g_log.error("generate session key failed")
            return 10115, "gen session key failed"

        value = {"numbers": numbers, "session_key": session_key, "create_time": datetime.fromtimestamp(timestamp),
                 "active_time": datetime.fromtimestamp(timestamp)}
        # session 入库
        # TODO... 如果登录模块分离，则考虑session存入redis
        collection = get_mongo_collection("session")
        if not collection:
            g_log.error("get collection session failed")
            return 10116, "get collection session failed"
        session = collection.find_one_and_update({"numbers": numbers}, {"$set": value})
        # 第一次更新，则插入一条
        if not session:
            g_log.debug("first login")
            session = collection.insert_one(value)
        if not session:
            g_log.error("%s login failed", numbers)
            return 10117, "login failed"
        g_log.debug("login succeed")

        return 10100, session_key
    except Exception as e:
        g_log.error("%s", e)
        return 10118, "exception"


def check_md5(plain, salt, cipher, times):
    """
    检查
    cipher = md5(plain) * times － 1
    cipher = md5(cipher + salt)
    cipher = base64_encode(a2b_hex(cipher))
    :param plain: 明文
    :param cipher: 密文
    :param times: 几次md5
    :return:
    """
    try:
        for i in xrange(0, times - 1):
            m = hashlib.md5()
            m.update(plain)
            plain = m.hexdigest()

        # 加盐
        m = hashlib.md5()
        m.update(plain + salt)
        plain = m.hexdigest()

        # base64压缩
        # TODO... 等前端搞定时使用
        # plain = binascii.a2b_hex(plain)
        plain = base64.b64encode(plain)

        if plain == cipher:
            return 0
        else:
            return 1
    except Exception as e:
        g_log.error("<%s> %s", e.__class__, e)
        return 1


def generate_md5(plain, salt, times):
    try:
        for i in xrange(0, times - 1):
            m = hashlib.md5()
            m.update(plain)
            plain = m.hexdigest()
            # g_log.debug(plain)

        # 加盐
        m = hashlib.md5()
        # g_log.debug(plain + salt)
        m.update(plain + salt)
        plain = m.hexdigest()

        # base64压缩
        # g_log.debug(plain)
        cipher = binascii.b2a_base64(plain)
        # g_log.debug(cipher)
        # TODO... 等前端搞定时使用
        # plain = binascii.a2b_hex(plain)
        # cipher = binascii.b2a_base64(plain)

        return cipher
    except Exception as e:
        g_log.error("<%s> %s", e.__class__, e)
        return ""

def register_request(**kwargs):
    """
    账号注册
    :param kwargs: {"numbers": "18688982240", "password": "123456", "password_md5": "c56d0e9a7ccec67b4ea131655038d604"}
    :return: (10200, "yes")/成功，(>10210, "errmsg")/失败
    """
    try:
        # 检查要创建的用户numbers
        numbers = kwargs.get("numbers", "")
        if not account_is_valid(numbers):
            g_log.warning("invalid account %s", numbers)
            return 10211, "invalid account"

        # TODO... 检查密码字符
        password = kwargs.get("password", "")
        password_md5 = kwargs.get("password_md5", "")

        # password_md5 = md5(md5(md5(password)))
        if 1 == check_md5(password, numbers, password_md5, 3):
            g_log.warning("password_md5 != md5(md5(md5(password)))")
            return 10212, "invalid password"

        value = {"numbers": numbers, "password": password, "password_md5": password_md5,
                 "deleted": 0, "time": datetime.now(), "update_time": datetime.now()}
        # 存入数据库
        collection = get_mongo_collection("account")
        if not collection:
            g_log.error("get collection account failed")
            return 10214, "get collection account failed"
        account = collection.find_one_and_update({"numbers": numbers}, {"$set": value})

        # 第一次更新，则插入一条
        if not account:
            g_log.debug("register new account")
            account = collection.insert_one(value)
        if not account:
            g_log.error("register account %s failed", numbers)
            return 10215, "register account failed"
        g_log.debug("register succeed")

        # TODO... 增加客户或商家资料信息
        return 10200, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 10216, "exception"


def change_password_request(**kwargs):
    """
    重置密码
    :param kwargs: {"numbers": "18688982240", "password": "123456", "password_md5": "c56d0e9a7ccec67b4ea131655038d604"}
    :return: (10300, "yes")/成功，(>10310, "errmsg")/失败
    """
    try:
        # 检查要创建的用户numbers
        numbers = kwargs.get("numbers", "")
        if not account_is_valid(numbers):
            g_log.warning("invalid account %s", numbers)
            return 10311, "invalid account"

        password = kwargs.get("password", "")
        password_md5 = kwargs.get("password_md5", "")

        # password_md5 = md5(md5(md5(password)))
        if 1 == check_md5(password, password_md5, 3):
            g_log.warning("password_md5 != md5(md5(md5(password)))")
            return 10312, "invalid password"

        value = {"numbers": numbers, "password": password, "password_md5": password_md5,
                 "deleted": 0, "update_time": datetime.now()}
        # 更新数据库
        collection = get_mongo_collection("account")
        if not collection:
            g_log.error("get collection account failed")
            return 10314, "get collection account failed"
        account = collection.find_one_and_update({"numbers": numbers}, {"$set": value})
        if not account:
            g_log.error("account %s change password failed", numbers)
            return 10315, "change password failed"
        g_log.debug("change password succeed")

        return 10300, "yes"
    except Exception as e:
        g_log.error("%s", e)
        return 10316, "exception"


def verify_session_key(account, session_key):
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
            return 10411, "illegal session key"
        (timestamp, numbers) = plain
        if account != numbers:
            g_log.error("not account %s session", account)
            return 10412, "account not match"

        # session 落地验证
        collection = get_mongo_collection("session")
        if not collection:
            g_log.error("get collection session failed")
            return 10413, "get collection session failed"
        create_time = datetime.fromtimestamp(timestamp)
        session = collection.find_one({"numbers": numbers, "session_key": session_key, "create_time": create_time})
        if not session:
            g_log.debug("session %s not exist", session_key)
            return 10414, "session not exist"

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
        return 10415, "exception"


def identity_to_numbers(identity):
    """
    账号ID转换成账号
    :param identity: 账号ID
    :return:
    """
    try:
        if not identity:
            return 10514, "illegal identity"
        collection = get_mongo_collection("account")
        if not collection:
            g_log.error("get collection account failed")
            return 10511, "get collection account failed"
        account = collection.find_one({"_id": ObjectId(identity), "deleted": 0})
        if not account:
            g_log.debug("account %s not exist", identity)
            return 10512, "account not exist"
        numbers = account["numbers"]
        return 10500, numbers
    except Exception as e:
        g_log.error("%s", e)
        return 10513, "exception"


def numbers_to_identity(numbers):
    """
    账号转换成账号ID
    :param numbers: 账号
    :return:
    """
    try:
        collection = get_mongo_collection("account")
        if not collection:
            g_log.error("get collection account failed")
            return 10514, "get collection account failed"
        account = collection.find_one({"numbers": numbers, "deleted": 0})
        if not account:
            g_log.debug("account %s not exist", numbers)
            return 10515, "account not exist"
        identity = str(account["identity"])
        return 10500, identity
    except Exception as e:
        g_log.error("%s", e)
        return 10516, "exception"


if "__main__" == __name__:
    # result1 = verify_session_key("18688982240", "7KiWrPOQgmvMpjsIVIVmr0ulNYFN4vSfWCKjGg==")
    # g_log.debug(result1)
    g_log.debug("%s", generate_md5("123456", "118688982241", 3))