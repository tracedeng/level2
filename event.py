__author__ = 'tracedeng'


import gevent
from gevent.event import Event
import time
'''
Illustrates the use of events
'''

evt = Event()


def setter():
    """After 3 seconds, wake all threads waiting on the value of evt"""
    print('A: Hey wait for me, I have to do something')
    gevent.sleep(3)
    print("Ok, I'm done")
    evt.set()
    # gevent.sleep(3)
    # time.sleep(3)
    # for i in xrange(0, 50):
    #     print("after set")
    # evt.set()


def waiter():
    """After 3 seconds the get call will unblock"""
    t = gevent.Timeout(4)
    t.start()
    try:
        for i in xrange(0, 2):
            print("I'll wait for you")
            evt.clear()
            evt.wait()  # blocking
            print("It's about time")
    except Exception as e:
        print(e)
    except gevent.Timeout as e:
        print(e)
    # evt.clear()
    # evt.wait()
    # print("It's about time, again")


def main():
    gevent.joinall([
        gevent.spawn(setter),
        # gevent.spawn(waiter),
        # gevent.spawn(waiter),
        # gevent.spawn(waiter),
        # gevent.spawn(waiter),
        gevent.spawn(waiter)
    ])


if __name__ == '__main__':
    main()