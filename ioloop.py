__author__ = 'tracedeng'


#coding=utf8
import socket
import gevent
from gevent.core import loop

def f():
    print('hsfe')
    s, address = sock.recvfrom(1024)
    # s, address = sock.accept()
    print address
    # s.send("hello world\r\n")

loop = loop()
# sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock.bind(("localhost", 9527))
# sock.listen(10)
io = loop.io(sock.fileno(),1) #1
io.start(f)
loop.run()
