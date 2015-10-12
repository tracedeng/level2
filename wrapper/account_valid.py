# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


def user_is_valid(user, mode):
    """
    商家用户phoneNumber = 10000000000 + phoneNumber，区分个人用户
    :param user:
    :param mode: 账号类型，1/普通用户，2/商家用户
    :return: 0/无效，1/有效
    """
    if mode == 1:
        return phone_number_is_valid(user)
    elif mode == 2:
        if user[0] != "1":
            return 0
        user = int(user) - 10000000000
        return phone_number_is_valid(user)


def user_is_valid_consumer(user):
    return user_is_valid(user, 1)


def user_is_valid_merchant(user):
    return user_is_valid(user, 2)


def phone_number_is_valid(user):
    return 1