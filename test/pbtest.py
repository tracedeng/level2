__author__ = 'tracedeng'


import struct
import socket
import common_pb2
# import lv2.register_pb2


address = ('127.0.0.1', 9527)

req = common_pb2.Request()
# print(dir(register_req))
req.head.cmd = 1
req.head.seq = 2
req.head.phone_number = "18688982240"

req.register.phone_number = "18688982240"
req.register.password = "123456"
req.register.password_md5 = "asdasfkjakwjef"
req_serial = req.SerializeToString()

length = len(req_serial) + 4
f = "!I%ss" % length
message = struct.pack(f, length, "{{%s}}" % req_serial)
# message = struct.pack("!I12s", 12, "{{abcdefg}}")

sock = socket.socket(type=socket.SOCK_DGRAM)
sock.connect(address)

print('Sending %s bytes to %s:%s' % ((len(message), ) + address))
# print('%s' % message)
print('%s' % req)
sock.send(message.encode())

data, address = sock.recvfrom(8192)
print('recv %s bytes from %s:%s' % ((len(data), ) + address))
# print('%s' % data)
response = common_pb2.Response()
response.ParseFromString(data[6:-2])
print('%s' % response)
