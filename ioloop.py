__author__ = 'tracedeng'


#coding=utf8
import socket
import gevent
from gevent.core import loop

def f(x):
    def f2():
        print('%s', x)
        if x == '9529':
            print("user sock2")
            s, address = sock2.recvfrom(1024)
        else:
            print("user sock1")
            s, address = sock.recvfrom(1024)
        # print dir(io)
        # print dir(io2)
        # print dir(loop)
        # s, address = sock.accept()
        # print address
        # s.send("hello world\r\n")
    return f2

loop = loop()
# sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock.bind(("localhost", 9530))
# sock.listen(10)
io = loop.io(sock.fileno(), 1) #1
# io.args = ('c', 'd')
io.start(f('9530'))

sock2 = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock2.bind(("localhost", 9529))
# sock.listen(10)
io2 = loop.io(sock2.fileno(), 1) #1
# io2.args = ('a', 'b')
io2.start(f('9529'))

loop.run()
