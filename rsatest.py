# -*- coding: utf-8 -*-
__author__ = 'tracedeng'


# from rsa import encrypt, PublicKey, PrivateKey
# import binascii
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA
# from Crypto.Cipher import PKCS1_v1_5, PKCS1_OAEP
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


if __name__ == "__main__":
#     data = "body=商家充值"
    data = "body=商家充值&buyer_email=18688982240&buyer_id=2088702468082645&discount=0.00&gmt_create=" \
           "2016-06-06 17:52:27&gmt_payment=2016-06-06 17:52:28&is_total_fee_adjust=N" \
           "&notify_id=a73d4110bb40cbe85cd246bb2ee8788kxu&notify_time=2016-06-06 17:52:28" \
           "&notify_type=trade_status_sync&out_trade_no=b00cb774cf792157e7f789d73cb35089" \
           "&payment_type=1&price=0.01&quantity=1&seller_email=biiyooit@qq.com" \
           "&seller_id=2088221780225801&subject=商家充值&total_fee=0.01&trade_no=2016060621001004640213805346" \
           "&trade_status=TRADE_SUCCESS&use_coupon=N"
    ci = base64.b64decode("amd9WkyaiPTDvn30GIISzIZyuKQI5CpQ4AZwEwWwyV9Uq7Qc1FCQsUNE8TlJ/kNLs39kVycNHcz1IA7HLi+Wu5AHO+/JpBOp8gW8vwHyx1cCgz7NGXXQZCb6d23YKz8UcOayrAS6j44PvqCjSI+is4WnNabhZBEU28czVEmB560=")

    checksign(data, ci)