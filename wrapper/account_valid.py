# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


def user_is_valid(user, mode):
    """
    商家用户phoneNumber = 10000000000 + phoneNumber，区分个人用户
    :param user:
    :param mode: 账号类型，1/普通用户，2/商家用户
    :return: 0/无效，1/有效
    """
    if mode == UserMode.MERCHANT:
        if user[0] != "1":
            return 0
        user = int(user) - 10000000000

    return phone_number_is_valid(user)


def user_is_valid_consumer(user):
    return user_is_valid(user, UserMode.CONSUMER)


def user_is_valid_merchant(user):
    return user_is_valid(user, UserMode.MERCHANT)


def phone_number_is_valid(user):
    return 1


class UserMode():
    CONSUMER = 1
    MERCHANT = 2

    def __init__(self):
        pass


def sexy_string_2_number(sexy):
    string2number = {'female': 1, 'male': 2}
    return string2number.get(sexy, 0)


def sexy_number_2_string(sexy):
    number2string = {'1': 'female', '2': 'male'}
    return number2string.get(str(sexy), 'unknow')


def yes_no_2_char(yes_no):
    yesno2char = {"yes": "y", "no": "n"}
    return yesno2char.get(yes_no.lower(), 'n')


def char_2_yes_no(yes_no):
    char2yesno = {"y": "yes", "n": "no"}
    return char2yesno.get(yes_no.lower(), 'no')