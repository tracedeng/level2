__author__ = 'tracedeng'

# from __future__ import print_function
import sys
from gevent import socket
import struct

address = ('182.254.159.219', 9528)
address = ('127.0.0.1', 9529)
# address = ('139.196.41.147', 9528)
# address  = ('223.104.5.229', 27551)
#message = ' '.join(sys.argv[1:])
#message = "@860719120000028,20150901113010,12123.84983,3110.84462,,,626,5,60,74,39983"
# message = "860719020065236,7,182.254.159.219,9527"


# 6166,62113,-65
# message = "$357713009999993,20150901113010,6166,62113,23,39983,626,5,60,74"
# message = "@860719120000028,20150901113010,12123.84983,3110.84462,19,87,23,39983,626,5,60,74"
message = "#860719120000034,24,34,25,35,26,36,27,37,224,134,125,135,216,336,272,317,24,34,25,35,26,36,27,37,22,224,134,125,135,216,336,272,317,24,34,25,35,26,36,27,37,22,224,134,125,135,216,336,272,317,24,34,25,35,26,36,27,37,22,224,134,125,135,216,336,272,317,24,34,25,35,26,36,27,37,224,134,125,135,216,336,272,317,24,34,25,35,26,36,27,37,224,134,125,135,216,336,272,317"
# message = "!860719120000028,21,1"

# message = "860719120000034,7,117.131.3.2197,26762"
# message = "860719120000034,19,117.131.3.2197,26762,182.182.182.182,5000"
# message = "860719120000099,21,116.226.47.82,54699"
# import struct
# message = struct.pack("!I12s", 12, "{{abcdefgh}}")

sock = socket.socket(type=socket.SOCK_DGRAM)
sock.connect(address)

print('Sending %s bytes to %s:%s' % ((len(message), ) + address))
print('%s' % message)
sock.send(message.encode())

# data, address = sock.recvfrom(8192)
# print('recv %s bytes from %s:%s' % ((len(data), ) + address))
# print('%s' % data)


#data, address = sock.recvfrom(8192)
#print('%s:%s: got %r' % (address + (data, )))
# def foo():
#     sock2 = socket.socket(type=socket.SOCK_DGRAM)
#     sock2.connect(address)
#     print('Sending %s bytes to %s:%s' % ((len(message), ) + address))
#     sock2.send(message.encode())


#foo()
