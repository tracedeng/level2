# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import common_pb2
import log
g_log = log.WrapperLog('stream', name=__name__, level=log.DEBUG).log  # 启动日志功能
import branch_socket
import re

from redis_connection import get_redis_connection

from account_valid import *

import pymongo

from bson.objectid import ObjectId


import time
from random import Random

class Login():
    """
    注册登录模块，命令号<100
    request：请求包解析后的pb格式
    """
    def __init__(self, request):
        self.request = request
        self.head = request.head
        self.cmd = request.head.cmd
        self.seq = request.head.seq


    def enter(self):
        """
        处理具体业务
        :return: 0/不回包给前端，pb/正确返回，timeout/超时
        """


        try:
            print "start ... try..."
            command_handle = {2: self.user_register, 3: self.user_login, 5:self.change_password, 6: self.send_verifycode, 7: self.verify_code}
            result = command_handle.get(self.cmd, self.dummy_command)()
            if result == 0:
                # 错误或者异常，不回包
                response = 0
            elif result == 1:
                # 错误，且回包
                pass
                #response = package.error_response(self.cmd, self.seq, self.code, self.message)
            else:
                # 正确，回包
                response = result

            return response
        except Exception as e:
            g_log.error("%s", e)
            return 0

    #用户注册
    def user_register(self):
        """
        1 请求字段有效性检查
        2 验证登录态
        3 检查是否已创建的consumer
        4 consumer写入数据库
        :return: 0/不回包给前端，pb/正确返回，1/错误，并回错误包
        """

        try:
            register = self.request.register_request
            phone_number = register.phone_number
            password = register.password
            password_md5 = register.password_md5

            #注册返回信息
            response = common_pb2.Response()
            response.head.cmd = self.head.cmd
            response.head.seq = self.head.seq
            response.head.code = 1002
            response.head.message = ""
            recordtime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))


            print password
            print password_md5

            #判断手机号码格式正确
            if phone_number_is_valid(phone_number) <> 1 :
                response.head.message = "手机号码格式错误"
                response.head.code = 100310
                return response

            #判断密码格式正确

            conn = pymongo.MongoClient("127.0.0.1",27017)
            db = conn.test #连接库

            content = db.client_password.find_one({"user_id":phone_number})
            if content:
                response.head.message = "手机号码已经注册"
                response.head.code = 100210
                return response
            else:

                try:
                    db.client_password.insert({"user_id":phone_number,"password":password,"password_md5":password_md5})
                    response.head.message = "register succeed"
                    response.head.code = 100201

                except Exception as e:
                    g_log.error("%s", e)
                    return 0





            g_log.debug("%s", response)
            return response

        except Exception as e:
            g_log.error("%s", e)
            return 0

    #用户登录
    def user_login(self):
        try:
            login = self.request.login_request
            phone_number = login.phone_number
            password = login.password
            skey = random_str(32)

            response = common_pb2.Response()
            response.head.cmd = self.head.cmd
            response.head.seq = self.head.seq
            response.head.code = 1003
            response.head.message = ""
            recordtime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))

            #判断手机号码格式正确
            if phone_number_is_valid(phone_number) <> 1 :
                response.head.message = "手机号码格式错误"
                response.head.code = 100310
                return response

            #判断密码格式正确

            conn = pymongo.MongoClient("127.0.0.1",27017)
            db = conn.test #连接库

            content = db.client_password.find_one({"user_id":phone_number,"password":password})

            if content:
                """
                存储登录信息
                """
                #login_status = db.user_login_status.find_one({"user_id":phone_number,"avaliable_time":{"$gt":recordtime}})
                login_status = db.user_login_status.find_one({"user_id":phone_number})

                if login_status:

                    db.user_login_status.update({"user_id":phone_number},{"$set":{"skey":skey,"update_time":recordtime}})
                    response.head.message = "relogin succeed"
                    response.login_response.skey = skey
                    response.head.code = 100302

                else:
                    db.user_login_status.insert({
                    "user_id":phone_number,"skey":skey,"login_time":recordtime,"update_time":recordtime})
                    response.head.message = "login succeed"
                    response.login_response.skey = skey
                    response.head.code = 100301

            else:
                response.head.message = "手机号码或密码错误"
                response.head.code = 100311

            g_log.debug("%s", response)
            return response



        except Exception as e:
            g_log.error("%s", e)
            return 0
        pass

    #验证验证码
    def verify_code(self):
        try:
            verifycode = self.request.verify_request
            phone_number = verifycode.phone_number
            code = verifycode.code

            response = common_pb2.Response()
            response.head.cmd = self.head.cmd
            response.head.seq = self.head.seq
            response.head.code = 1007
            response.head.message = ""
            recordtime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))


            #判断手机号码格式正确
            if phone_number_is_valid(phone_number) <> 1 :
                response.head.message = "手机号码格式错误"
                response.head.code = 100310
                return response


            conn = pymongo.MongoClient("127.0.0.1",27017)
            db = conn.test #连接库

            content = db.user_verifycode.find_one({"user_id":phone_number})

            if content:
                print content

                #判断时间是否超过1分钟，如果超过一分钟则显示 验证码超时
                if 1 == 0:
                    response.head.message = "verify out of time"
                    response.head.code = 100711
                else:
                    #如果验证码没超时，则验证验证码是否相同
                    if code == content["verifycode"]:

                        #删除验证码记录
                        db.user_verifycode.remove({"user_id":phone_number})
                        response.head.message = "verify sucessful"
                        response.head.code = 100701
                    else:
                        response.head.message = "verifycode error"
                        response.head.code = 100710


            else:
                response.head.message = "此手机号码未发送验证码或验证码超时"
                response.head.code = 100311



            g_log.debug("%s", response)
            return response

        except Exception as e:
            g_log.error("%s", e)
            return 0

    #发送验证码
    def send_verifycode(self):
        try:
            verifycode = self.request.verify_request
            phone_number = verifycode.phone_number


            response = common_pb2.Response()
            response.head.cmd = self.head.cmd
            response.head.seq = self.head.seq
            response.head.code = 1006
            response.head.message = ""
            recordtime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))

            #判断手机号码格式正确
            if phone_number_is_valid(phone_number) <> 1 :
                response.head.message = "手机号码格式错误"
                response.head.code = 100310
                return response

            #触发验证服务器，发送验证码到相应的手机上,保存验证码的有效时间到服务器上/需要解决过期验证码的自动删除问题
            #发送验证码功能代码，获取验证码值
            verifycode = 9889


            conn = pymongo.MongoClient("127.0.0.1",27017)
            db = conn.test #连接库


            try:
                db.user_verifycode.remove({"user_id":phone_number});
                db.user_verifycode.insert({
                    "user_id":phone_number,"verifycode":verifycode,"request_time":recordtime})
                response.head.message = "send verifycode succeed"
                response.head.code = 100601
            except Exception as e:
                g_log.error("%s", e)
                return 0

            return response


        except Exception as e:
            g_log.error("%s", e)
            return 0

    #忘记密码，走提交验证码，重置密码流程

    #修改密码
    def change_password(self):
        try:
            register = self.request.password_request
            phone_number = register.phone_number
            password = register.password
            password_md5 = register.password_md5

            #注册返回信息
            response = common_pb2.Response()
            response.head.cmd = self.head.cmd
            response.head.seq = self.head.seq
            response.head.code = 1005
            response.head.message = ""
            recordtime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))

            print "|||||||||||||"

            print self.searchphonenumber("5629b9f9b8a7c10cfc70c8f1")
            print "|||||||||||||"

            #判断手机号码格式正确
            if phone_number_is_valid(phone_number) <> 1 :
                response.head.message = "手机号码格式错误"
                response.head.code = 100310
                return response

            #判断密码格式正确

            conn = pymongo.MongoClient("127.0.0.1",27017)
            db = conn.test #连接库

            try:
                db.client_password.update({"user_id":phone_number},{"$set":{"password":password,"password_md5":password_md5}})
                response.head.message = "change password succeed"
                response.password_response.change_result = "change succeed"
                response.head.code = 100501
            except Exception as e:
                g_log.error("%s", e)
                return 0

            g_log.debug("%s", response)
            return response

        except Exception as e:
            g_log.error("%s", e)
            return 0

    #查找电话号码
    def searchphonenumber(self, id):
        try:

            #验证ID 格式正确，否则返回100811

            conn = pymongo.MongoClient("127.0.0.1",27017)
            db = conn.test #连接库

            content = db.client_password.find_one({"_id":ObjectId (id)})

            if content:
                return 100801,content["user_id"]
            else:
                return 100810,"id is not exist"

        except Exception as e:
            g_log.error("%s", e)
            return 0

    #验证登录态
    def checklogin(self,phonoe_number,skey):
        try:

            #验证phonenumber,skey  格式正确，否则返回100911

            conn = pymongo.MongoClient("127.0.0.1",27017)
            db = conn.test #连接库

            content = db.user_login_status.find_one({"user_id":phonoe_number,"skey":skey})
            if content:
                if 1 == 1:
                    #判断在登录有效期内
                    return 100901,"skey is ok"
                else:
                    return 100912,"login out of time"
            else:
                return 100910,"phonenumber not login or skey unavaliable"

        except Exception as e:
            g_log.error("%s", e)
            return 0





    def dummy_command(self):
        # 无效的命令，不回包
        g_log.debug("unknow command %s", self.cmd)
        return 0



def enter(request):
    """
    登录模块入口
    :param request: 解析后的pb格式
    :return: 0/不回包给前端，pb/正确返回，timeout/超时
    """
    try:
        login = Login(request)
        return login.enter()
    except Exception as e:
        g_log.error("%s", e)
        return 0


"""
共用函数，需要更新到lib共用库
"""
def random_str(randomlength=8):
    str = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    length = len(chars) - 1
    random = Random()
    for i in range(randomlength):
        str+=chars[random.randint(0, length)]
    return str

def compare_time(start_t,end_t):
    s_time = time.mktime(time.strptime(start_t,'%Y-%m-%d %H:%M:%S'))
    e_time = time.mktime(time.strptime(end_t,'%Y-%m-%d %H:%M:%S'))
    if (float(s_time) <= float(e_time)):
        return True
    return False
