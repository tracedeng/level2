# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import qrcode
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
from qiniu_token import upload_token_retrieve_debug, upload_file, upload_data


def generate_qrcode(value):
    """
    生成二维码
    :param value: 二维码内容
    :return: 暂时存在本地文件，文件名"qrcode.png", 不返回
    """
    qr = qrcode.QRCode(version=2)
    qr.add_data(value)
    qr.make(fit=True)
    img = qr.make_image()
    img.save("qrcode.png")


def upload_merchant_qrcode(numbers, merchant_identity):
    """
    创建商家时，生成商家二维码，存入七牛，二维码内容是商家ID
    :param numbers: 管理员
    :param merchant_identity: 商家ID
    :return:
    """
    errcode, msg = upload_token_retrieve_debug("m_qrcode", numbers, merchant_identity)
    token = msg[0]
    key = msg[1]
    # g_log.debug(token)
    # g_log.debug(key)
    generate_qrcode(merchant_identity)
    code, message = upload_file(token, key, "qrcode.png")
    if code == 70400:
        return 1, message["key"]
    else:
        return 0, message


if __name__ == "__main__":
    print upload_merchant_qrcode("18688982240", "abcdefghijklmn")