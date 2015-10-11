__author__ = 'tracedeng'

# from __future__ import print_function
from gevent.server import DatagramServer
import time


class EchoServer(DatagramServer):

    def handle(self, data, address):
        print('%s: got %r' % (address[0], data))
        # print data.split(",")
        # self.socket.sendto('Received %s bytes' % len(data), address)
        time.sleep(1.1)
        self.socket.sendto(data, address)
        print('send data back')


if __name__ == '__main__':
    print('Receiving datagrams on :9528')
    EchoServer(':9528').serve_forever()

