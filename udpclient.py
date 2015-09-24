__author__ = 'tracedeng'

# from __future__ import print_function
import sys
from gevent import socket
import google.protobuf

address = ('182.254.159.219', 9527)
address = ('127.0.0.1', 9527)
# address  = ('223.104.5.229', 27551)
#message = ' '.join(sys.argv[1:])
#message = "@860719120000028,20150901113010,12123.84983,3110.84462,,,626,5,60,74,39983"
# message = "860719020065236,7,182.254.159.219,9527"

# message = "$860719120000032,20150919135212,6338,62449,28,,0,0,63,3905"
# message = ""
#message = "#860719120000028,20150911010101,3,24,34,25,35,26,36,27,37"
#message = "860719120000034,7,117.131.3.2197,26762"
import struct

message = struct.pack("!I12s", 12, "{{abcdefgh}}")

sock = socket.socket(type=socket.SOCK_DGRAM)
sock.connect(address)
print('Sending %s bytes to %s:%s' % ((len(message), ) + address))
print('%s' % message)

sock.send(message.encode())
data, address = sock.recvfrom(8192)
#data = sock.recvfrom(1024)
#print('recv %s bytes' % len(data))
print('recv %s bytes from %s:%s' % ((len(data), ) + address))
print('%s' % data)


#data, address = sock.recvfrom(8192)
#print('%s:%s: got %r' % (address + (data, )))
# def foo():
#     sock2 = socket.socket(type=socket.SOCK_DGRAM)
#     sock2.connect(address)
#     print('Sending %s bytes to %s:%s' % ((len(message), ) + address))
#     sock2.send(message.encode())


#foo()
