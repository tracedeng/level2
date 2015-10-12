__author__ = 'tracedeng'

import gevent
from gevent import Greenlet

"""
def foo(message, n):
    gevent.sleep(n)
    print(message)

# Initialize a new Greenlet instance running the named function
# foo
thread1 = Greenlet.spawn(foo, "Hello", 1)

# Wrapper for creating and running a new Greenlet from the named
# function foo, with the passed arguments
thread2 = gevent.spawn(foo, "I live!", 2)

# Lambda expressions
thread3 = gevent.spawn(lambda x: (x+1), 2)

threads = [thread1, thread2, thread3]

# Block until all threads complete.
gevent.joinall(threads)
"""


def foo():
    print "foo"
    print gevent.getcurrent()
    gevent.sleep(2)
    return "foo"

def foo2(green):
    print("foo2")
    print gevent.getcurrent()
    return "foo2"


print gevent.getcurrent()
t = Greenlet(foo)
print t.ready()
t.start()
t.link(foo2)
t.join(0)
#t.kill()

print "yes"
print t.ready()
print t.successful()
#print t.get()
print t.value