# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import linecache
import sys
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能


def print_exception():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    line_no = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    # line = linecache.getline(filename, line_no, f.f_globals)
    # return filename, line_no
    g_log.debug("%s, %s", filename, line_no)
    # print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, line_no, line.strip(), exc_obj)

