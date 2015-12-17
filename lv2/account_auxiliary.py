# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


from mongo_connection import get_mongo_collection
from bson.objectid import ObjectId
import base64
import binascii
import hashlib
from xxtea import decrypt, encrypt
from datetime import datetime
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能


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
            g_log.debug(plain)

        # 加盐
        g_log.debug(plain + salt)
        m = hashlib.md5()
        m.update(plain + salt)
        plain = m.hexdigest()
        g_log.debug(plain)

        # base64压缩
        # TODO... 等前端搞定时使用
        plain = binascii.b2a_base64(plain)
        plain = plain.strip()
        g_log.debug(plain)
        g_log.debug(cipher)

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