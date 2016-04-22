# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

from datetime import datetime, date, timedelta


def get_day(delta):
    """
    获取几天前或者几天后
    :param delta: 天，> 0 / 几天后， < 0 / 几天前
    :return:
    """
    # today = date.today()
    today = datetime.now()
    delta_day = timedelta(days=-delta)

    return today - delta_day


def after_weeks(weeks):
    return datetime.strptime(str(get_day(7 * weeks)), '%Y-%m-%d %H:%M:%S.%f')
    # return get_day(7 * weeks)


def yesterday():
    return datetime.strptime(str(get_day(-1)), '%Y-%m-%d %H:%M:%S.%f')
    # return get_day(-1)


def after_years(years):
    # today = date.today()
    today = datetime.now()
    # return today.replace(today.year + years)
    return datetime.strptime(str(today.replace(today.year + years)), '%Y-%m-%d %H:%M:%S.%f')

