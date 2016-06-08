# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


# from rsa import encrypt, PublicKey, PrivateKey
# import binascii
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA
from Crypto.Cipher import PKCS1_v1_5 as CIPHER_PKCS1_v1_5, PKCS1_OAEP
from Crypto.Signature import PKCS1_v1_5
import base64
# from OpenSSL.crypto import FILETYPE_PEM, load_privatekey, sign


def checksign(data, sign):
    # sign = base64.b64decode(sign)
    verifier = PKCS1_v1_5.new(RSA.importKey(open('rsa_alipay_public_key.pem', 'r').read()))
    if verifier.verify(SHA.new(data), sign):
        print "The signature is authentic."
    else:
        print "The signature is not authentic."


def rsa_order(order):
    """
    订单rsa加密
    :return:
    """
    # order = {"partner": "2088221780225801", "seller_id": "biiyooit@qq.com", "out_trade_no": trade_no,
    #          "subject": "商家充值", "body": "商家充值", "total_fee": str(money),
    #          "notify_url": "http://www.weijifen.me:8000/flow", "service": "mobile.securitypay.pay",
    #          "payment_type": "1", "_input_charset": "utf-8", "it_b_pay": "30m"}

    plain = ""
    for key in sorted(order):
        plain += "&%s=%s" % (key, order[key])

    print(plain[1:])
    print(len(plain))
    # cipher = PKCS1_v1_5.new(RSA.importKey(open('./rsa_private_key_pkcs8.pem', 'r').read()))
    # cipher2 = cipher.sign(plain[1:])
    cipher = CIPHER_PKCS1_v1_5.new(RSA.importKey(open('./rsa_private_key.pem', 'r').read()))
    # cipher = CIPHER_PKCS1_v1_5.new(RSA.importKey(open('./rsa_private_key_pkcs8.pem', 'r').read()))
    # cipher = base64.b64encode(cipher.encrypt(plain))
    print(cipher.can_encrypt())
    cipher2 = cipher.encrypt(plain[0:])
    print len(cipher2)
    return base64.b64encode(cipher2)
    # return base64.b64encode(cipher.encrypt(plain[1:]))


def rsa_order2(order):
    from OpenSSL.crypto import load_privatekey, FILETYPE_PEM, sign, load_certificate

    print order
    plain = ""
    for key in sorted(order):
        plain += "&%s=%s" % (key, order[key])

    print(plain[1:])
    print(len(plain))
    key = load_privatekey(1, open("rsa_private_key_pkcs8.pem").read())
    # print(key)
    d = sign(key, plain, 'sha1')
    print(len(d))
    return base64.b64encode(d)


if __name__ == "__main__":
        # data = "body=商家充值"
    # data = "body=商家充值&buyer_email=18688982240&buyer_id=2088702468082645&discount=0.00&gmt_create=" \
    #        "2016-06-06 17:52:27&gmt_payment=2016-06-06 17:52:28&is_total_fee_adjust=N" \
    #        "&notify_id=a73d4110bb40cbe85cd246bb2ee8788kxu&notify_time=2016-06-06 17:52:28" \
    #        "&notify_type=trade_status_sync&out_trade_no=b00cb774cf792157e7f789d73cb35089" \
    #        "&payment_type=1&price=0.01&quantity=1&seller_email=biiyooit@qq.com" \
    #        "&seller_id=2088221780225801&subject=商家充值&total_fee=0.01&trade_no=2016060621001004640213805346" \
    #        "&trade_status=TRADE_SUCCESS&use_coupon=N"
    # ci = base64.b64decode("amd9WkyaiPTDvn30GIISzIZyuKQI5CpQ4AZwEwWwyV9Uq7Qc1FCQsUNE8TlJ/kNLs39kVycNHcz1IA7HLi+Wu5AHO+/JpBOp8gW8vwHyx1cCgz7NGXXQZCb6d23YKz8UcOayrAS6j44PvqCjSI+is4WnNabhZBEU28czVEmB560=")
    # checksign(data, ci)

    order = {"partner": "2088221780225801", "seller_id": "biiyooit@qq.com", "out_trade_no": "90ajf823jfad",
             "subject": "商家充值", "body": "商家充值", "total_fee": "200",
             "notify_url": "http://www.weijifen.me:8000/flow", "service": "mobile.securitypay.pay",
             "payment_type": "1", "_input_charset": "utf-8", "it_b_pay": "30m"}
    print rsa_order2(order)
    # print order