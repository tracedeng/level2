# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


import logging
import logging.handlers

NOTSET = logging.NOTSET
DEBUG = logging.DEBUG
INFO = logging.INFO
WARN = logging.WARN
WARNING = WARN
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

class WrapperLog(object):
    """
    logging模块wrapper
    """
    HOST = '127.0.0.1'
    PORT = 9527
    FILENAME = 'data.log'
    MAXBYTES = 1014 * 1024
    BACKUPCOUNT = 5

    def __init__(self, *args, **kwargs):
        """
        初始化log wrapper类，并行支持udp，文件，标准输出三种模式
        udp模式：带上HOST，PORT参数，否则使用127.0.0.1:9527
        file模式：带上file参数，否则使用当前目录下data.log文件，带上maxbytes，否则1M，带上backupcount，否则5个
        stream模式：标准输出
        日志level：level参数表示logger日志级别，缺省WARNING，各handle级别小于logger级别无效
        不支持自定义输出格式
        :param args: [udp, file, stream, ...]
        :param kwargs: {'host': '127.0.0.1', 'port': 9527, 'file': 'xxx.log', 'level': logging.DEBUG,
            'udplevel': logging.DEBUG, 'filelevel': logging.DEBUG, 'streamlevel': logging.DEBUG}
        :return:WrapperLog
        """

        args = list(set(args))  # 去重
        self.level = kwargs.get('level', logging.WARNING)

        name = kwargs.get('name', 'unknow')
        # self.log = logging.getLogger(__name__)
        self.log = logging.getLogger(name)
        self.log.setLevel(self.level)
        self.log.propagate = False  # 不传递
        handles = []
        d = {'udp': self._init_udp_handle, 'stream': self._init_stream_handle, 'file': self._init_file_handle}
        for handle in args:
            handle = d.get(handle, self._init_null_handle)(**kwargs)
            # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            # handle.format(formatter)
            handles.append(handle)

        for handle in handles:
            self.log.addHandler(handle)

    def _init_file_handle(self, **kwargs):
        file_name = kwargs.get('filename', self.__class__.FILENAME)
        max_bytes = kwargs.get('maxbytes', self.__class__.MAXBYTES)
        backup_count = kwargs.get('backupcount', self.__class__.BACKUPCOUNT)

        handle = logging.handlers.RotatingFileHandler(file_name, max_bytes, backup_count)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
        handle.setFormatter(formatter)
        handle.setLevel(kwargs.get('filelevel', self.level))
        return handle

    def _init_udp_handle(self, **kwargs):
        host = kwargs.get('host', self.__class__.HOST)
        port = kwargs.get('port', self.__class__.PORT)
        handle = logging.handlers.DatagramHandler(host, port)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
        handle.setFormatter(formatter)
        handle.setLevel(kwargs.get('udplevel', self.level))
        return handle

    def _init_stream_handle(self, **kwargs):
        handle = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
        handle.setFormatter(formatter)
        handle.setLevel(kwargs.get('streamlevel', self.level))
        return handle

    def _init_null_handle(self, **kwargs):
        handle = logging.NullHandler()
        return handle

    # 封装logging，没法获取行号，勿用
    def debug(self, msg, *args, **kwargs):
        self.log.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.log.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.log.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.log.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.log.critical(msg, *args, **kwargs)

if '__main__' == __name__:
    wrapper_log = WrapperLog('stream', 'udp', level=logging.DEBUG, streamlevel=logging.DEBUG)
    wrapper_log.debug("This is debug message")
    wrapper_log.info("This is info message")
    wrapper_log.warning("This is warning message")
