__author__ = 'tracedeng'


import struct
import socket
import google.protobuf

address = ('127.0.0.1', 9527)

message = struct.pack("!I12s", 12, "{{abcdefg}}")

sock = socket.socket(type=socket.SOCK_DGRAM)
sock.connect(address)

print('Sending %s bytes to %s:%s' % ((len(message), ) + address))
print('%s' % message)
sock.send(message.encode())