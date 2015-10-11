# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import random
import gevent
from gevent.event import AsyncResult

import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from singleton import singleton


# TODO... UUID超时
# 单例类，import时路径要一致，否则起不到效果
@singleton
class CoroutineManage():
    """
    维护协程id和对应的事件字典
    支持主动yield和switch协程
    协程退出时需调用drop_coroutine_uuid
    coroutine_uuid = {'uuid': evt, 'uuid2': evt2, 'uuid3': evt3}
    """
    # SEQUENCEMAX = 99999
    # DEFAULTUUID = 'defaultuuid'
    # UUIDRANDOMCHAR = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

    def __init__(self, *args, **kwargs):
        self.sequence_max = 99999
        self.default_uuid = 'defaultuuid'
        self.uuid_random_char = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        self.coroutine_uuid = {}
        self.unique_sequence = 1

    def generate_coroutine_uuid(self):
        """
        生成协程的唯一ID，对应gevent事件
        :return: 0/随机算法有问题、需立刻处理，uuid/正确随机生成协程唯一ID
        """
        uuid = self._generate_uuid()
        g_log.debug("generate coroutine id %s", uuid)
        times = 1
        # 超过三次获取随机id失败，认为系统随机算法有问题
        while uuid in self.coroutine_uuid:
            g_log.warning("coroutine uuid(%s) duplicate, %d", uuid, times)
            if ++times > 4:
                g_log.critical("generate coroutine uuid failed")
                return 0
            uuid = self._generate_uuid()
            g_log.debug("generate coroutine id %s", uuid)
        # self.coroutine_uuid[uuid] = Event()
        self.coroutine_uuid[uuid] = AsyncResult()
        return uuid

    def drop_coroutine_uuid(self, uuid):
        """
        协程结束时，清除该协程的uuid对应的事件
        :param uuid: 要清除的协程id
        :return:
        """
        try:
            if uuid in self.coroutine_uuid:
                evt = self.coroutine_uuid.pop(uuid)
                g_log.debug("clear coroutine %s event", uuid)
        except Exception as e:
            g_log.error("%s", e)

    def coroutine_switch(self, uuid, message, kill_self=True):
        """
        切换到当前协程
        :param uuid: 要切换到的协程
        :param message: 协程间传递的消息
        :param kill_self: 是否结束当前协程
        :return:
        """
        try:
            evt = self.coroutine_uuid.get(uuid)
            if evt:
                g_log.debug("switch to coroutine %s", uuid)
                evt.set(message)
            else:
                g_log.warning("coroutine %s not exist, maybe timeout", uuid)
        except Exception as e:
            g_log.error("%s", e)
        finally:
            if kill_self:
                g_log.debug("kill self coroutine")
                gevent.kill(gevent.getcurrent())

    def coroutine_yield(self, uuid, timeout=None):
        """
        将当前协程切换出去，回到hub
        :param uuid: 要被切换的协程id
        :return: 当前协程被唤醒时传递过来的消息/正确，0/错误，timeout/超时
        """
        try:
            evt = self.coroutine_uuid.get(uuid)
            if evt:
                g_log.debug("yield coroutine %s", uuid)
                # evt.clear() # 清除已set的事件
                # evt.wait()
                # g_log.critical("timeout:%s", timeout)
                return evt.get(timeout=timeout)
                # evt.get()
            else:
                g_log.critical("coroutine %s not exist, kill current coroute", uuid)
                return 0
        except gevent.Timeout as e:
            # 用超时时间来区分不同的超时处理逻辑，不匹配需raise
            if e.seconds != timeout:
                raise e
            g_log.debug("current coroutine timeout")
            return "timeout"
        except Exception as e:
            g_log.error("%s", e)
            return 0

    def _generate_uuid(self):
        """
        生成唯一的协程ID，0_0_0_0_0_，0代表一位数字［0-9］，_代表一个字符［a-zA-Z］,考虑协程数最多100,000个，
        用5位数表示，位与位之间嵌入随即的字符
        :return: uuid
        """
        try:
            self.unique_sequence = 1 if self.unique_sequence > self.sequence_max else self.unique_sequence
            sequence = self.unique_sequence
            left = 10
            l = []
            while sequence > 0:
                l.append(str(sequence % 10))
                l.append(random.choice(self.uuid_random_char))
                sequence /= 10
                left -= 2
            l.extend(random.sample(self.uuid_random_char, left))
            uuid = ''.join(l)
        except Exception as e:
            g_log.error("%s", e)
            uuid = self.default_uuid
        finally:
            self.unique_sequence += 1
        return uuid


# 语法糖
def generate_coroutine_uuid():
    """
    生成UUID
    :return: 0/失败，uuid/成功
    """
    try:
        manager = CoroutineManage()
        return manager.generate_coroutine_uuid()
    except Exception as e:
        g_log.error("%s", e)
        return 0


def coroutine_yield(uuid, timeout=None):
    """
    切换出当前协程
    :param uuid:
    :return: 当前协程被唤醒时传递过来的消息/正确，0/错误，timeout/超时
    """
    try:
        manager = CoroutineManage()
        return manager.coroutine_yield(uuid, timeout)
    except Exception as e:
        g_log.error("%s", e)
        return 0


def coroutine_switch(uuid, message, kill_self=True):
    """
    切换到UUID对应的协程，切换失败交给协程超时逻辑
    :param uuid: 协程标志
    :param message: 传递给协程的消息
    :param kill_self: 是否结束自身
    :return:
    """
    try:
        manager = CoroutineManage()
        manager.coroutine_switch(uuid, message)
    except Exception as e:
        g_log.error("%s", e)


def drop_coroutine_event(uuid):
    """
    删除当前协程UUID
    :param uuid:
    :return:
    """
    manager = CoroutineManage()
    manager.drop_coroutine_uuid(uuid)


if "__main__" == __name__:
    manager1 = CoroutineManage()
    for i in xrange(0, 20):
        manager1.generate_coroutine_uuid()
    manager2 = CoroutineManage()
    for i in xrange(0, 20):
        manager2.generate_coroutine_uuid()