# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


def message_has_field(message, field):
    """
    message是否有field，解决"Protocol message has no non-repeated submessage field" 问题
    :param message: pb中定义的message
    :param field: message中的某个field
    :return: 0/没有，1/有
    """
    fields = []
    for descriptor in message.ListFields():
        fields.append(descriptor[0].name)

    if field in fields:
        return 1
    return 0