__author__ = 'tracedeng'



import gevent
from gevent.event import AsyncResult
from gevent.event import Event
a = AsyncResult()
a.rawlink(foo)
c = Event()

def setter():
    """
    After 3 seconds set the result of a.
    """
    gevent.sleep(2)
    # c.set()
    a.set('Hello!')
    # c.set()

def waiter():
    """
    After 3 seconds the get call will unblock after the setter
    puts a value into the AsyncResult.
    """
    try:
        print(a)
        b = a.get(timeout=1)
        print(a)
        print(b)
    except gevent.Timeout as e:
        print(a)
        print(e)

    # for i in xrange(0, 5):
    #     try:
    #         b = a.get(timeout=1)
    #         print(b)
    #     except gevent.Timeout as e:
    #         print(e)


gevent.joinall([
    gevent.spawn(setter),
    gevent.spawn(waiter),
])