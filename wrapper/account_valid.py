# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


def _account_is_valid(account, mode):
    """
    商家用户phoneNumber = 20000000000 + phoneNumber
    个人用户phoneNumber = 10000000000 + phoneNumber
    :param account:
    :param mode: 账号类型，1/普通用户，2/商家用户
    :return: 0/无效，1/有效
    """
    if mode == AccountMode.MERCHANT:
        if account[0] != "2":
            return 0
        account = int(account) - 200000000000
    elif mode == AccountMode.CONSUMER:
        if account[0] != "1":
            return 0
        account = int(account) - 100000000000
    else:
        return 0

    return numbers_is_valid(str(account))


def account_is_valid_consumer(account):
    """
    是否有效的客户账号
    :param account: 账号
    :return: 0/无效，1/有效
    """
    return _account_is_valid(account, AccountMode.CONSUMER)


def account_is_valid_merchant(account):
    """
    是否有效的商家账号
    :param account: 账号
    :return: 0/无效，1/有效
    """
    return _account_is_valid(account, AccountMode.MERCHANT)


def account_is_platform(account):
    """
    是否有效的平台账号
    :param account: 账号
    :return: 0/无效，1/有效
    """
    return 1


def numbers_is_valid(numbers):
    """
    检查是否有效手机号
    :param numbers: 手机号
    :return: 0/无效，1/有效
    """
    if len(numbers) != 11:
        return 0
    return 1


def account_is_valid(account):
    """
    账号是否有效
    :param account: 账号 
    :return: 0/无效，1/有效
    """
    if account[0] == "1":
        return account_is_valid_consumer(account)
    elif account[0] == "2":
        return account_is_valid_merchant(account)


def numbers_to_account(numbers, mode):
    """
    电话号码按照类型转换成账号
    :param numbers: 手机号
    :param mode: 账号类型
    :return: 有效账号/成功，None/失败
    """
    if 1 != numbers_is_valid(numbers):
        return None

    if mode == AccountMode.MERCHANT:
        account = int(numbers) + 200000000000
    elif mode == AccountMode.CONSUMER:
        account = int(numbers) + 100000000000
    else:
        return None

    return str(account)


class AccountMode():
    CONSUMER = 1
    MERCHANT = 2

    def __init__(self):
        pass


def sexy_string_2_number(sexy):
    """
    性别转对应的数字
    :param sexy:
    :return:
    """
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